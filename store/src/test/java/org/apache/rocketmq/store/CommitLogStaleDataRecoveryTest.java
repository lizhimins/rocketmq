/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Recovery behaviour when a CommitLog file's blocks hold content from an older generation.
 *
 * <p>This is the failure mode introduced by allocating a new file's extents without zeroing them: the
 * bytes become readable as soon as the extents are marked written, and on a broker's own volume those
 * bytes are almost always a CommitLog file that retention just deleted. Such content parses as valid
 * messages and passes CRC, so the only reliable discriminator is whether each message sits at the
 * physical offset it claims.
 *
 * <p>Both recovery paths must apply that rule; the "check disabled" cases below pin the unsafe
 * behaviour so that enabling the no-zero-fill allocation path without the check is visibly wrong.
 */
public class CommitLogStaleDataRecoveryTest extends StoreTestBase {

    private static final int COMMIT_LOG_FILE_SIZE = 1024 * 64;
    private static final int MESSAGE_COUNT = 200;
    private static final String TOPIC = "staleRecoverTopic";

    /**
     * Writes enough messages to span several CommitLog files, shuts the store down, overwrites the
     * newest file with the oldest file's content, then recovers.
     *
     * @return {offsetOfLastFile, maxPhyOffsetAfterRecovery}
     */
    private long[] recoverAfterInjectingStaleData(boolean checkOffset, boolean abnormal) throws Exception {
        String rootDir = System.getProperty("java.io.tmpdir") + File.separator
            + "stale-recover-" + UUID.randomUUID();
        baseDirs.add(rootDir);

        DefaultMessageStore store = buildStore(rootDir, checkOffset);
        assertThat(store.load()).isTrue();
        store.start();
        try {
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                MessageExtBrokerInner msg = buildMessage();
                msg.setTopic(TOPIC);
                msg.setQueueId(0);
                PutMessageResult result = store.putMessage(msg);
                assertThat(result.getPutMessageStatus()).isEqualTo(PutMessageStatus.PUT_OK);
            }
            StoreTestUtil.waitCommitLogReput(store);
        } finally {
            store.shutdown();
        }

        List<File> files = commitLogFiles(rootDir);
        assertThat(files.size())
            .as("at least two CommitLog files are needed to model an old file's blocks being reused")
            .isGreaterThanOrEqualTo(2);

        File oldest = files.get(0);
        File newest = files.get(files.size() - 1);
        long newestFileOffset = Long.parseLong(newest.getName());
        assertThat(newestFileOffset).isGreaterThan(Long.parseLong(oldest.getName()));

        overwriteWith(newest, oldest);

        if (abnormal) {
            // Unclean shutdown: leaving the abort file makes recovery take the abnormal path.
            File abortFile = new File(StorePathConfigHelper.getAbortFile(rootDir));
            UtilAll.ensureDirOK(abortFile.getParent());
            assertThat(abortFile.createNewFile()).isTrue();
        }

        DefaultMessageStore recovered = buildStore(rootDir, checkOffset);
        assertThat(recovered.load()).isTrue();
        recovered.start();
        try {
            return new long[] {newestFileOffset, recovered.getMaxPhyOffset()};
        } finally {
            recovered.shutdown();
        }
    }

    private DefaultMessageStore buildStore(String rootDir, boolean checkOffset) throws Exception {
        MessageStoreConfig config = new MessageStoreConfig();
        config.setStorePathRootDir(rootDir);
        config.setMappedFileSizeCommitLog(COMMIT_LOG_FILE_SIZE);
        config.setMappedFileSizeConsumeQueue(1024 * 4);
        config.setMaxHashSlotNum(100);
        config.setMaxIndexNum(100 * 10);
        config.setHaListenPort(0);
        config.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        config.setCheckCommitLogOffsetOnRecover(checkOffset);
        return new DefaultMessageStore(config, new BrokerStatsManager("staleRecoverTest", true),
            new NoopMessageArrivingListener(), new BrokerConfig(), new ConcurrentHashMap<>());
    }

    private static class NoopMessageArrivingListener implements MessageArrivingListener {
        @Override
        public void arriving(String topic, int queueId, long logicOffset, long tagsCode,
            long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {
        }
    }

    private static List<File> commitLogFiles(String rootDir) {
        File dir = new File(rootDir + File.separator + "commitlog");
        File[] listed = dir.listFiles();
        assertThat(listed).as("commitlog directory missing: %s", dir).isNotNull();
        List<File> files = new ArrayList<>(Arrays.asList(listed));
        files.sort((a, b) -> a.getName().compareTo(b.getName()));
        return files;
    }

    /** Copies the whole content of {@code src} over {@code dst}, keeping dst's length. */
    private static void overwriteWith(File dst, File src) throws Exception {
        try (RandomAccessFile srcRaf = new RandomAccessFile(src, "r");
             RandomAccessFile dstRaf = new RandomAccessFile(dst, "rw");
             FileChannel srcChannel = srcRaf.getChannel();
             FileChannel dstChannel = dstRaf.getChannel()) {
            long size = Math.min(srcChannel.size(), dstChannel.size());
            long done = 0;
            while (done < size) {
                done += srcChannel.transferTo(done, size - done, dstChannel);
            }
            dstChannel.force(true);
        }
    }

    @Test
    public void staleDataRejectedOnNormalRecoveryTest() throws Exception {
        long[] r = recoverAfterInjectingStaleData(true, false);
        assertThat(r[1])
            .as("with the strict check, recovery must stop before the stale data instead of "
                + "accepting an older generation's messages as current")
            .isLessThanOrEqualTo(r[0]);
    }

    @Test
    public void staleDataRejectedOnAbnormalRecoveryTest() throws Exception {
        long[] r = recoverAfterInjectingStaleData(true, true);
        assertThat(r[1])
            .as("abnormal recovery must apply the same rule as normal recovery")
            .isLessThanOrEqualTo(r[0]);
    }

    @Test
    public void staleDataAcceptedWhenCheckDisabledOnNormalRecoveryTest() throws Exception {
        long[] r = recoverAfterInjectingStaleData(false, false);
        assertThat(r[1])
            .as("with the check off, stale data is accepted as current messages; this is exactly why "
                + "the check must be on before enabling allocation without zeroing")
            .isGreaterThan(r[0]);
    }

    @Test
    public void staleDataAcceptedWhenCheckDisabledOnAbnormalRecoveryTest() throws Exception {
        long[] r = recoverAfterInjectingStaleData(false, true);
        assertThat(r[1])
            .as("abnormal recovery accepts stale data too when the check is off")
            .isGreaterThan(r[0]);
    }
}

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
package org.apache.rocketmq.tieredstore.file;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.MessageStoreExecutor;
import org.apache.rocketmq.tieredstore.metadata.FileSegmentMetadata;
import org.apache.rocketmq.tieredstore.metadata.MetadataManager;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtilTest;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TieredCommitLogTest {

    private final String storePath = MessageStoreTest.getRandomStorePath();
    private MessageQueue mq;
    private TieredFileAllocator fileAllocator;
    private MetadataStore metadataStore;

    @Before
    public void setUp() throws ClassNotFoundException, NoSuchMethodException {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setBrokerName("brokerName");
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredStoreFilePath(storePath + File.separator);
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.provider.posix.PosixFileSegment");
        storeConfig.setCommitLogRollingInterval(0);
        storeConfig.setTieredStoreCommitLogMaxSize(1000);

        metadataStore = new MetadataManager(storeConfig);
        fileAllocator = new TieredFileAllocator(metadataStore, storeConfig);
        mq = new MessageQueue("CommitLogTest", storeConfig.getBrokerName(), 0);
        MessageStoreExecutor.init();
    }

    @After
    public void tearDown() throws IOException {
        MessageStoreTest.deleteStoreDirectory(storePath);
        MessageStoreExecutor.shutdown();
    }

    @Test
    public void correctMinOffsetTest() {
        String filePath = TieredStoreUtil.toPath(mq);
        TieredCommitLog tieredCommitLog = new TieredCommitLog(fileAllocator, filePath);
        Assert.assertEquals(0L, tieredCommitLog.getMinOffset());
        Assert.assertEquals(0L, tieredCommitLog.getCommitOffset());
        Assert.assertEquals(0L, tieredCommitLog.getCommitConsumeQueueOffset());

        // append some messages
        for (int i = 6; i < 50; i++) {
            ByteBuffer byteBuffer = MessageFormatUtilTest.buildMockedMessageBuffer();
            byteBuffer.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, i);
            Assert.assertEquals(AppendResult.SUCCESS, tieredCommitLog.append(byteBuffer));
        }

        tieredCommitLog.commit(true);
        tieredCommitLog.correctMinOffset();

        // single file store: 1000 / 122 = 8, file count: 44 / 8 = 5
        Assert.assertEquals(6, tieredCommitLog.getFlatFile().getFileSegmentCount());

        metadataStore.iterateFileSegment(filePath, FileSegmentType.COMMIT_LOG, metadata -> {
            if (metadata.getBaseOffset() < 1000) {
                metadata.setStatus(FileSegmentMetadata.STATUS_DELETED);
                metadataStore.updateFileSegment(metadata);
            }
        });

        // manually delete file
        List<TieredFileSegment> segmentList = tieredCommitLog.getFlatFile().getFileSegmentList();
        segmentList.remove(0).destroyFile();
        segmentList.remove(0).destroyFile();

        tieredCommitLog.correctMinOffset();
        Assert.assertEquals(4, tieredCommitLog.getFlatFile().getFileSegmentCount());
        Assert.assertEquals(6 + 8 + 8, tieredCommitLog.getMinConsumeQueueOffset());
    }
}
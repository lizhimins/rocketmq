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
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.FlatMessageStoreTest;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.metadata.DefaultMetadataStore;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FlatCommitLogFileTest {

    private final String storePath = FlatMessageStoreTest.getRandomStorePath();
    private MessageQueue mq;
    private FlatFileFactory fileAllocator;
    private MetadataStore metadataStore;

    @Before
    public void setUp() throws ClassNotFoundException, NoSuchMethodException {
        MessageStoreConfig storeConfig = new MessageStoreConfig();
        storeConfig.setBrokerName("brokerName");
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredStoreFilePath(storePath + File.separator);
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.provider.PosixFileSegment");
        storeConfig.setCommitLogRollingInterval(0);
        storeConfig.setTieredStoreCommitLogMaxSize(1000);

        metadataStore = new DefaultMetadataStore(storeConfig);
        fileAllocator = new FlatFileFactory(metadataStore, storeConfig);
        mq = new MessageQueue("CommitLogTest", storeConfig.getBrokerName(), 0);
//        MessageStoreExecutor.init();
    }

    @After
    public void tearDown() throws IOException {
        FlatMessageStoreTest.deleteStoreDirectory(storePath);
//        MessageStoreExecutor.shutdown();
    }

    @Test
    public void correctMinOffsetTest() {
//        String filePath = TieredStoreUtil.toPath(mq);
//        FlatCommitLogFile flatCommitLogFile = new FlatCommitLogFile(fileAllocator, filePath);
//        Assert.assertEquals(0L, flatCommitLogFile.getMinOffset());
//        Assert.assertEquals(0L, flatCommitLogFile.getCommitOffset());
//        Assert.assertEquals(0L, flatCommitLogFile.getCommitConsumeQueueOffset());
//
//        // append some messages
//        for (int i = 6; i < 50; i++) {
//            ByteBuffer byteBuffer = MessageFormatUtilTest.buildMockedMessageBuffer();
//            byteBuffer.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, i);
//            Assert.assertEquals(AppendResult.SUCCESS, flatCommitLogFile.append(byteBuffer));
//        }
//
//        flatCommitLogFile.commit(true);
//        flatCommitLogFile.correctMinOffset();
//
//        // single file store: 1000 / 122 = 8, file count: 44 / 8 = 5
//        Assert.assertEquals(6, flatCommitLogFile.getFlatFile().getFileSegmentCount());
//
//        metadataStore.iterateFileSegment(filePath, FileSegmentType.COMMIT_LOG, metadata -> {
//            if (metadata.getBaseOffset() < 1000) {
//                metadata.setStatus(FileSegmentMetadata.STATUS_DELETED);
//                metadataStore.updateFileSegment(metadata);
//            }
//        });
//
//        // manually delete file
//        List<FileSegment> segmentList = flatCommitLogFile.getFlatFile().getFileSegmentList();
//        segmentList.remove(0).destroyFile();
//        segmentList.remove(0).destroyFile();
//
//        flatCommitLogFile.correctMinOffset();
//        Assert.assertEquals(4, flatCommitLogFile.getFlatFile().getFileSegmentCount());
//        Assert.assertEquals(6 + 8 + 8, flatCommitLogFile.getMinConsumeQueueOffset());
    }
}
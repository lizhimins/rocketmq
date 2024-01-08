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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.tieredstore.MessageStoreTest;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.metadata.DefaultMetadataStore;
import org.apache.rocketmq.tieredstore.metadata.entity.QueueMetadata;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.provider.MemoryFileSegment;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtilTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FlatMessageFileExtTest {

    private final String storePath = MessageStoreTest.getRandomStorePath();
    private MessageStoreConfig storeConfig;
    private MetadataStore metadataStore;
    private FlatFileFactory flatFileFactory;
    private MessageQueue mq;

    @Before
    public void setUp() throws ClassNotFoundException, NoSuchMethodException {
        storeConfig = new MessageStoreConfig();
        storeConfig.setBrokerName("brokerName");
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.provider.MemoryFileSegment");
        storeConfig.setCommitLogRollingInterval(0);
        storeConfig.setCommitLogRollingMinimumSize(999);
        mq = new MessageQueue("CompositeQueueFlatFileTest", storeConfig.getBrokerName(), 0);
        metadataStore = new DefaultMetadataStore(storeConfig);
        flatFileFactory = new FlatFileFactory(metadataStore, storeConfig);
//        MessageStoreExecutor.init();
    }

    @After
    public void tearDown() throws IOException {
        MessageStoreTest.deleteStoreDirectory(storePath);
//        MessageStoreExecutor.shutdown();
    }

    @Test
    public void testAppendCommitLog() {
        FlatMessageFileExt flatFile = new FlatMessageFileExt(flatFileFactory, mq);
        ByteBuffer message = MessageFormatUtilTest.buildMockedMessageBuffer();
        AppendResult result = flatFile.appendCommitLog(message);
        Assert.assertEquals(AppendResult.SUCCESS, result);
        Assert.assertEquals(123L, flatFile.commitLog.getFlatFile().getFileToWrite().getAppendPosition());
        Assert.assertEquals(0L, flatFile.commitLog.getFlatFile().getFileToWrite().getCommitPosition());

        flatFile = new FlatMessageFileExt(flatFileFactory, mq);
        flatFile.initOffset(6);
        result = flatFile.appendCommitLog(message);
        Assert.assertEquals(AppendResult.SUCCESS, result);

        message.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, 7);
        result = flatFile.appendCommitLog(message);
        Assert.assertEquals(AppendResult.SUCCESS, result);

        flatFile.commitCommitLog();
        flatFile.commitConsumeQueue();
//        Assert.assertEquals(7, flatFile.getCommitLogDispatchCommitOffset());

        flatFile.cleanExpiredFile(0);
        flatFile.destroyExpiredFile();
    }

    @Test
    public void testAppendConsumeQueue() {
//        FlatMessageFileExt file = new FlatMessageFileExt(flatFileFactory, mq);
//        DispatchRequest request = new DispatchRequest(
//            mq.getTopic(), mq.getQueueId(), 51, 2, 3, 4);
//        AppendResult result = file.appendConsumeQueue(request);
//        Assert.assertEquals(AppendResult.OFFSET_INCORRECT, result);
//
//        // Create new segment in file queue
//        MemoryFileSegment segment = new MemoryFileSegment(FileSegmentType.CONSUME_QUEUE, mq, 20, storeConfig);
//        segment.initPosition(segment.getSize());
//        file.consumeQueue.getFlatFile().setBaseOffset(20L);
//        file.consumeQueue.getFlatFile().getFileToWrite();
//
//        // Recreate will load metadata and build consume queue
//        file = new FlatMessageFileExt(flatFileFactory, mq);
//        segment.initPosition(ConsumeQueue.CQ_STORE_UNIT_SIZE);
//        result = file.appendConsumeQueue(request);
//        Assert.assertEquals(AppendResult.SUCCESS, result);
//
//        request = new DispatchRequest(
//            mq.getTopic(), mq.getQueueId(), 52, 2, 3, 4);
//        result = file.appendConsumeQueue(request);
//        Assert.assertEquals(AppendResult.SUCCESS, result);
//
//        file.commitCommitLog();
//        file.commitConsumeQueue();
//        file.flushMetadata();
//
//        QueueMetadata queueMetadata = metadataStore.getQueue(mq);
//        Assert.assertEquals(53, queueMetadata.getMaxOffset());
    }

    @Test
    public void testBinarySearchInQueueByTime() throws ClassNotFoundException, NoSuchMethodException {

        // replace provider, need new factory again
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.provider.MemoryFileSegmentWithoutCheck");
        flatFileFactory = new FlatFileFactory(metadataStore, storeConfig);

        // inject store time: 0, +100, +100, +100, +200
        FlatMessageFileExt flatFile = new FlatMessageFileExt(flatFileFactory, mq);
        flatFile.initOffset(50);
        long timestamp1 = System.currentTimeMillis();
        ByteBuffer buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
        buffer.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, 50);
        buffer.putLong(MessageFormatUtil.STORE_TIMESTAMP_POSITION, timestamp1);
        flatFile.appendCommitLog(buffer);

        long timestamp2 = timestamp1 + 100;
        buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
        buffer.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, 51);
        buffer.putLong(MessageFormatUtil.STORE_TIMESTAMP_POSITION, timestamp2);
        flatFile.appendCommitLog(buffer);
        buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
        buffer.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, 52);
        buffer.putLong(MessageFormatUtil.STORE_TIMESTAMP_POSITION, timestamp2);
        flatFile.appendCommitLog(buffer);
        buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
        buffer.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, 53);
        buffer.putLong(MessageFormatUtil.STORE_TIMESTAMP_POSITION, timestamp2);
        flatFile.appendCommitLog(buffer);

        long timestamp3 = timestamp2 + 100;
        buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
        buffer.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, 54);
        buffer.putLong(MessageFormatUtil.STORE_TIMESTAMP_POSITION, timestamp3);
        flatFile.appendCommitLog(buffer);

        // append message to consume queue
//        flatFile.consumeQueue.getFlatFile().setBaseOffset(50 * ConsumeQueue.CQ_STORE_UNIT_SIZE);
//
//        for (int i = 0; i < 5; i++) {
//            AppendResult appendResult = flatFile.appendConsumeQueue(new DispatchRequest(
//                mq.getTopic(), mq.getQueueId(), MessageFormatUtilTest.MSG_LEN * i,
//                MessageFormatUtilTest.MSG_LEN, 0, timestamp1, 50 + i,
//                "", "", 0, 0, null));
//            Assert.assertEquals(AppendResult.SUCCESS, appendResult);
//        }

        // commit message will increase max consume queue offset
        flatFile.commitCommitLog();
        flatFile.commitConsumeQueue();

        Assert.assertEquals(54, flatFile.getOffsetInConsumeQueueByTime(timestamp3 + 1, BoundaryType.UPPER));
        Assert.assertEquals(54, flatFile.getOffsetInConsumeQueueByTime(timestamp3, BoundaryType.UPPER));

        Assert.assertEquals(50, flatFile.getOffsetInConsumeQueueByTime(timestamp1 - 1, BoundaryType.LOWER));
        Assert.assertEquals(50, flatFile.getOffsetInConsumeQueueByTime(timestamp1, BoundaryType.LOWER));

        Assert.assertEquals(51, flatFile.getOffsetInConsumeQueueByTime(timestamp1 + 1, BoundaryType.LOWER));
        Assert.assertEquals(51, flatFile.getOffsetInConsumeQueueByTime(timestamp2, BoundaryType.LOWER));
        Assert.assertEquals(54, flatFile.getOffsetInConsumeQueueByTime(timestamp2 + 1, BoundaryType.LOWER));
        Assert.assertEquals(54, flatFile.getOffsetInConsumeQueueByTime(timestamp3, BoundaryType.LOWER));

        Assert.assertEquals(50, flatFile.getOffsetInConsumeQueueByTime(timestamp1, BoundaryType.UPPER));
        Assert.assertEquals(50, flatFile.getOffsetInConsumeQueueByTime(timestamp1 + 1, BoundaryType.UPPER));
        Assert.assertEquals(53, flatFile.getOffsetInConsumeQueueByTime(timestamp2, BoundaryType.UPPER));
        Assert.assertEquals(53, flatFile.getOffsetInConsumeQueueByTime(timestamp2 + 1, BoundaryType.UPPER));

        Assert.assertEquals(0, flatFile.getOffsetInConsumeQueueByTime(timestamp1 - 1, BoundaryType.UPPER));
        Assert.assertEquals(55, flatFile.getOffsetInConsumeQueueByTime(timestamp3 + 1, BoundaryType.LOWER));
    }
}

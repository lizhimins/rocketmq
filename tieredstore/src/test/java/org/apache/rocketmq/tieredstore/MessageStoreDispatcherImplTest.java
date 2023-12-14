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
package org.apache.rocketmq.tieredstore;

import java.io.IOException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataManager;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.junit.After;
import org.junit.Before;

public class MessageStoreDispatcherImplTest {

    private final String storePath = MessageStoreTest.getRandomStorePath();
    private TieredMessageStoreConfig storeConfig;
    private MessageQueue mq;
    private TieredMetadataStore metadataStore;

    @Before
    public void setUp() {
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.provider.memory.MemoryFileSegmentWithoutCheck");
        storeConfig.setBrokerName(storeConfig.getBrokerName());
        mq = new MessageQueue("CompositeQueueFlatFileTest", storeConfig.getBrokerName(), 0);
        metadataStore = new TieredMetadataManager(storeConfig);
        TieredStoreExecutor.init();
    }

    @After
    public void tearDown() throws IOException {
        MessageStoreTest.deleteStoreDirectory(storePath);
        TieredStoreExecutor.shutdown();
    }

    //@Test
    //public void testDispatch() {
    //    metadataStore.addQueue(mq, 6);
    //    MemoryFileSegment segment = new MemoryFileSegment(FileSegmentType.COMMIT_LOG, mq, 1000, storeConfig);
    //    segment.initPosition(segment.getSize());
    //
    //    String filePath1 = TieredStoreUtil.toPath(mq);
    //    FileSegmentMetadata segmentMetadata = new FileSegmentMetadata(
    //        filePath1, 1000, FileSegmentType.COMMIT_LOG.getType());
    //    metadataStore.updateFileSegment(segmentMetadata);
    //    metadataStore.updateFileSegment(segmentMetadata);
    //
    //    segment = new MemoryFileSegment(FileSegmentType.CONSUME_QUEUE, mq,
    //        6 * TieredConsumeQueue.CONSUME_QUEUE_STORE_UNIT_SIZE, storeConfig);
    //    FileSegmentMetadata segmentMetadata2 = new FileSegmentMetadata(
    //        filePath1, segment.getBaseOffset(), FileSegmentType.CONSUME_QUEUE.getType());
    //    metadataStore.updateFileSegment(segmentMetadata2);

    //    TieredFlatFileManager flatFileManager = TieredFlatFileManager.getInstance(storeConfig);
    //    DefaultMessageStore defaultMessageStore = Mockito.mock(DefaultMessageStore.class);
    //    MessageStoreDispatcherImpl dispatcher = new MessageStoreDispatcherImpl(defaultMessageStore, storeConfig);
    //
    //    SelectMappedBufferResult mockResult = new SelectMappedBufferResult(0, MessageBufferUtilTest.buildMockedMessageBuffer(), MessageBufferUtilTest.MSG_LEN, null);
    //    Mockito.when(defaultMessageStore.selectOneMessageByOffset(7, MessageBufferUtilTest.MSG_LEN)).thenReturn(mockResult);
    //    DispatchRequest request = new DispatchRequest(mq.getTopic(), mq.getQueueId(), 6, 7, MessageBufferUtilTest.MSG_LEN, 1);
    //    dispatcher.dispatch(request);
    //    Assert.assertNotNull(flatFileManager.getFlatFile(mq));
    //    Assert.assertEquals(7, Objects.requireNonNull(flatFileManager.getFlatFile(mq)).getDispatchOffset());
    //
    //    CompositeQueueFlatFile flatFile = flatFileManager.getOrCreateFlatFileIfAbsent(mq);
    //    Assert.assertNotNull(flatFile);
    //    flatFile.commit(true);
    //    Assert.assertEquals(6, flatFile.getConsumeQueueMaxOffset());
    //
    //    dispatcher.buildConsumeQueueAndIndexFile();
    //    Assert.assertEquals(7, flatFile.getConsumeQueueMaxOffset());
    //
    //    ByteBuffer buffer1 = MessageBufferUtilTest.buildMockedMessageBuffer();
    //    buffer1.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 7);
    //    flatFile.appendCommitLog(buffer1);
    //    ByteBuffer buffer2 = MessageBufferUtilTest.buildMockedMessageBuffer();
    //    buffer2.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 8);
    //    flatFile.appendCommitLog(buffer2);
    //    ByteBuffer buffer3 = MessageBufferUtilTest.buildMockedMessageBuffer();
    //    buffer3.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 9);
    //    flatFile.appendCommitLog(buffer3);
    //    flatFile.commitCommitLog();
    //    Assert.assertEquals(9 + 1, flatFile.getDispatchOffset());
    //    Assert.assertEquals(9, flatFile.getCommitLogDispatchCommitOffset());
    //
    //    dispatcher.doRedispatchRequestToWriteMap(AppendResult.SUCCESS, flatFile, 8, 8, 0, 0, buffer1);
    //    dispatcher.doRedispatchRequestToWriteMap(AppendResult.SUCCESS, flatFile, 9, 9, 0, 0, buffer2);
    //    dispatcher.buildConsumeQueueAndIndexFile();
    //    Assert.assertEquals(7, flatFile.getConsumeQueueMaxOffset());
    //
    //    dispatcher.doRedispatchRequestToWriteMap(AppendResult.SUCCESS, flatFile, 7, 7, 0, 0, buffer1);
    //    dispatcher.doRedispatchRequestToWriteMap(AppendResult.SUCCESS, flatFile, 8, 8, 0, 0, buffer2);
    //    dispatcher.doRedispatchRequestToWriteMap(AppendResult.SUCCESS, flatFile, 9, 9, 0, 0, buffer3);
    //    dispatcher.buildConsumeQueueAndIndexFile();
    //    Assert.assertEquals(6, flatFile.getConsumeQueueMinOffset());
    //    Assert.assertEquals(9 + 1, flatFile.getConsumeQueueMaxOffset());
    //}
    //
    //@Test
    //public void testDispatchByFlatFile() {
    //    metadataStore.addQueue(mq, 6);
    //    TieredFlatFileManager flatFileManager = TieredFlatFileManager.getInstance(storeConfig);
    //    DefaultMessageStore defaultStore = Mockito.mock(DefaultMessageStore.class);
    //    Mockito.when(defaultStore.getConsumeQueue(mq.getTopic(), mq.getQueueId())).thenReturn(Mockito.mock(ConsumeQueue.class));
    //    MessageStoreDispatcherImpl dispatcher = new MessageStoreDispatcherImpl(defaultStore, storeConfig);
    //
    //    Mockito.when(defaultStore.getMinOffsetInQueue(mq.getTopic(), mq.getQueueId())).thenReturn(0L);
    //    Mockito.when(defaultStore.getMaxOffsetInQueue(mq.getTopic(), mq.getQueueId())).thenReturn(9L);
    //
    //    // mock cq item, position = 7
    //    ByteBuffer cqItem = ByteBuffer.allocate(ConsumeQueue.CQ_STORE_UNIT_SIZE);
    //    cqItem.putLong(7);
    //    cqItem.putInt(MessageBufferUtilTest.MSG_LEN);
    //    cqItem.putLong(1);
    //    cqItem.flip();
    //    SelectMappedBufferResult mockResult = new SelectMappedBufferResult(0, cqItem, ConsumeQueue.CQ_STORE_UNIT_SIZE, null);
    //    Mockito.when(((ConsumeQueue) defaultStore.getConsumeQueue(mq.getTopic(), mq.getQueueId())).getIndexBuffer(6)).thenReturn(mockResult);
    //
    //    // mock cq item, position = 8
    //    cqItem = ByteBuffer.allocate(ConsumeQueue.CQ_STORE_UNIT_SIZE);
    //    cqItem.putLong(8);
    //    cqItem.putInt(MessageBufferUtilTest.MSG_LEN);
    //    cqItem.putLong(1);
    //    cqItem.flip();
    //    mockResult = new SelectMappedBufferResult(0, cqItem, ConsumeQueue.CQ_STORE_UNIT_SIZE, null);
    //    Mockito.when(((ConsumeQueue) defaultStore.getConsumeQueue(mq.getTopic(), mq.getQueueId())).getIndexBuffer(7)).thenReturn(mockResult);
    //
    //    mockResult = new SelectMappedBufferResult(0, MessageBufferUtilTest.buildMockedMessageBuffer(), MessageBufferUtilTest.MSG_LEN, null);
    //    Mockito.when(defaultStore.selectOneMessageByOffset(7, MessageBufferUtilTest.MSG_LEN)).thenReturn(mockResult);
    //
    //    ByteBuffer msg = MessageBufferUtilTest.buildMockedMessageBuffer();
    //    msg.putLong(MessageBufferUtil.QUEUE_OFFSET_POSITION, 7);
    //    mockResult = new SelectMappedBufferResult(0, msg, MessageBufferUtilTest.MSG_LEN, null);
    //    Mockito.when(defaultStore.selectOneMessageByOffset(8, MessageBufferUtilTest.MSG_LEN)).thenReturn(mockResult);
    //
    //    CompositeQueueFlatFile flatFile = flatFileManager.getOrCreateFlatFileIfAbsent(mq);
    //    Assert.assertNotNull(flatFile);
    //    flatFile.initOffset(7);
    //    dispatcher.dispatchFlatFile(flatFile);
    //    Assert.assertEquals(8, flatFileManager.getFlatFile(mq).getDispatchOffset());
    //}
}

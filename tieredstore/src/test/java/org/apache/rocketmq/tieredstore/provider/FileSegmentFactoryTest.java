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
package org.apache.rocketmq.tieredstore.provider;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtilTest;
import org.junit.Assert;
import org.junit.Test;

public class FileSegmentFactoryTest {

    public int baseOffset = 1000;

    public FileSegment createFileSegment(FileSegmentType fileType) {
        String brokerName = new MessageStoreConfig().getBrokerName();
        return new MemoryFileSegment(fileType, new MessageQueue("TieredFileSegmentTest", brokerName, 0),
            baseOffset, new MessageStoreConfig());
    }

    @Test
    public void testCommitLog() {
        FileSegment segment = createFileSegment(FileSegmentType.COMMIT_LOG);
        segment.initPosition(segment.getSize());
        long lastSize = segment.getSize();
        segment.append(MessageFormatUtilTest.buildMockedMessageBuffer(), 0);
        segment.append(MessageFormatUtilTest.buildMockedMessageBuffer(), 0);
        Assert.assertTrue(segment.needCommit());

        ByteBuffer buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
        long msg3StoreTime = System.currentTimeMillis();
        buffer.putLong(MessageFormatUtil.STORE_TIMESTAMP_POSITION, msg3StoreTime);
        long queueOffset = baseOffset * 1000L;
        buffer.putLong(MessageFormatUtil.QUEUE_OFFSET_POSITION, queueOffset);
        segment.append(buffer, msg3StoreTime);

        Assert.assertEquals(baseOffset, segment.getBaseOffset());
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 3, segment.getAppendOffset());
        Assert.assertEquals(0, segment.getMinTimestamp());
        Assert.assertEquals(msg3StoreTime, segment.getMaxTimestamp());

//        segment.setFull();
        segment.commit();
        Assert.assertFalse(segment.needCommit());
        Assert.assertEquals(segment.getAppendOffset(), segment.getCommitOffset());
//        Assert.assertEquals(queueOffset, segment.getDispatchCommitOffset());

        ByteBuffer msg1 = segment.read(lastSize, MessageFormatUtilTest.MSG_LEN);
        Assert.assertEquals(baseOffset + lastSize, MessageFormatUtil.getCommitLogOffset(msg1));

        ByteBuffer msg2 = segment.read(lastSize + MessageFormatUtilTest.MSG_LEN, MessageFormatUtilTest.MSG_LEN);
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN, MessageFormatUtil.getCommitLogOffset(msg2));

        ByteBuffer msg3 = segment.read(lastSize + MessageFormatUtilTest.MSG_LEN * 2, MessageFormatUtilTest.MSG_LEN);
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 2, MessageFormatUtil.getCommitLogOffset(msg3));

        ByteBuffer coda = segment.read(lastSize + MessageFormatUtilTest.MSG_LEN * 3, MessageFormatUtil.COMMIT_LOG_CODA_SIZE);
        Assert.assertEquals(msg3StoreTime, coda.getLong(4 + 4));
    }

    private ByteBuffer buildConsumeQueue(long commitLogOffset) {
        ByteBuffer cqItem = ByteBuffer.allocate(MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
        cqItem.putLong(commitLogOffset);
        cqItem.putInt(2);
        cqItem.putLong(3);
        cqItem.flip();
        return cqItem;
    }

    @Test
    public void testConsumeQueue() {
        FileSegment segment = createFileSegment(FileSegmentType.CONSUME_QUEUE);
        segment.initPosition(segment.getSize());
        long lastSize = segment.getSize();
        segment.append(buildConsumeQueue(baseOffset), 0);
        segment.append(buildConsumeQueue(baseOffset + MessageFormatUtilTest.MSG_LEN), 0);
        long cqItem3Timestamp = System.currentTimeMillis();
        segment.append(buildConsumeQueue(baseOffset + MessageFormatUtilTest.MSG_LEN * 2), cqItem3Timestamp);

        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE * 3, segment.getAppendOffset());
        Assert.assertEquals(0, segment.getMinTimestamp());
        Assert.assertEquals(cqItem3Timestamp, segment.getMaxTimestamp());

        segment.commit();
        Assert.assertEquals(segment.getAppendOffset(), segment.getCommitOffset());

        ByteBuffer cqItem1 = segment.read(lastSize, MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
        Assert.assertEquals(baseOffset, cqItem1.getLong());

        ByteBuffer cqItem2 = segment.read(lastSize + MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE, MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
        Assert.assertEquals(baseOffset + MessageFormatUtilTest.MSG_LEN, cqItem2.getLong());

        ByteBuffer cqItem3 = segment.read(lastSize + MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE * 2, MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
        Assert.assertEquals(baseOffset + MessageFormatUtilTest.MSG_LEN * 2, cqItem3.getLong());
    }

    @Test
    public void testCommitFailedThenSuccess() {
        long startTime = System.currentTimeMillis();
        MemoryFileSegment segment = (MemoryFileSegment) createFileSegment(FileSegmentType.COMMIT_LOG);
        long lastSize = segment.getSize();
        segment.setCheckSize(false);
        segment.initPosition(lastSize);
        segment.setSize((int) lastSize);

        ByteBuffer buffer1 = MessageFormatUtilTest.buildMockedMessageBuffer().putLong(
            MessageFormatUtil.PHYSICAL_OFFSET_POSITION, baseOffset + lastSize);
        ByteBuffer buffer2 = MessageFormatUtilTest.buildMockedMessageBuffer().putLong(
            MessageFormatUtil.PHYSICAL_OFFSET_POSITION, baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN);
        segment.append(buffer1, 0);
        segment.append(buffer2, 0);

        // Mock new message arrive
        segment.blocker = new CompletableFuture<>();
        new Thread(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Assert.fail(e.getMessage());
            }
            ByteBuffer buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
            buffer.putLong(MessageFormatUtil.PHYSICAL_OFFSET_POSITION, MessageFormatUtilTest.MSG_LEN * 2);
            buffer.putLong(MessageFormatUtil.STORE_TIMESTAMP_POSITION, startTime);
            segment.append(buffer, 0);
            segment.blocker.complete(false);
        }).start();

        // Commit failed
        segment.commit();
        segment.blocker.join();
        segment.blocker = null;

        // Copy data and assume commit success
        segment.getMemStore().put(buffer1);
        segment.getMemStore().put(buffer2);
        segment.setSize((int) (lastSize + MessageFormatUtilTest.MSG_LEN * 2));

        segment.commit();
        Assert.assertEquals(lastSize + MessageFormatUtilTest.MSG_LEN * 3, segment.getCommitPosition());
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 3, segment.getCommitOffset());
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 3, segment.getAppendOffset());

        ByteBuffer msg1 = segment.read(lastSize, MessageFormatUtilTest.MSG_LEN);
        Assert.assertEquals(baseOffset + lastSize, MessageFormatUtil.getCommitLogOffset(msg1));

        ByteBuffer msg2 = segment.read(lastSize + MessageFormatUtilTest.MSG_LEN, MessageFormatUtilTest.MSG_LEN);
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN, MessageFormatUtil.getCommitLogOffset(msg2));

        ByteBuffer msg3 = segment.read(lastSize + MessageFormatUtilTest.MSG_LEN * 2, MessageFormatUtilTest.MSG_LEN);
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 2, MessageFormatUtil.getCommitLogOffset(msg3));
    }

    @Test
    public void testCommitFailed3Times() {
        long startTime = System.currentTimeMillis();
        MemoryFileSegment segment = (MemoryFileSegment) createFileSegment(FileSegmentType.COMMIT_LOG);
        long lastSize = segment.getSize();
        segment.setCheckSize(false);
        segment.initPosition(lastSize);
        segment.setSize((int) lastSize);

        ByteBuffer buffer1 = MessageFormatUtilTest.buildMockedMessageBuffer().putLong(
            MessageFormatUtil.PHYSICAL_OFFSET_POSITION, baseOffset + lastSize);
        ByteBuffer buffer2 = MessageFormatUtilTest.buildMockedMessageBuffer().putLong(
            MessageFormatUtil.PHYSICAL_OFFSET_POSITION, baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN);
        segment.append(buffer1, 0);
        segment.append(buffer2, 0);

        // Mock new message arrive
        segment.blocker = new CompletableFuture<>();
        new Thread(() -> {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Assert.fail(e.getMessage());
            }
            ByteBuffer buffer = MessageFormatUtilTest.buildMockedMessageBuffer();
            buffer.putLong(MessageFormatUtil.PHYSICAL_OFFSET_POSITION, MessageFormatUtilTest.MSG_LEN * 2);
            buffer.putLong(MessageFormatUtil.STORE_TIMESTAMP_POSITION, startTime);
            segment.append(buffer, 0);
            segment.blocker.complete(false);
        }).start();

        for (int i = 0; i < 3; i++) {
            segment.commit();
        }

        Assert.assertEquals(lastSize, segment.getCommitPosition());
        Assert.assertEquals(baseOffset + lastSize, segment.getCommitOffset());
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 3, segment.getAppendOffset());

        segment.blocker.join();
        segment.blocker = null;

        segment.commit();
        Assert.assertEquals(lastSize + MessageFormatUtilTest.MSG_LEN * 2, segment.getCommitPosition());
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 2, segment.getCommitOffset());
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 3, segment.getAppendOffset());

        segment.commit();
        Assert.assertEquals(lastSize + MessageFormatUtilTest.MSG_LEN * 3, segment.getCommitPosition());
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 3, segment.getCommitOffset());
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 3, segment.getAppendOffset());

        ByteBuffer msg1 = segment.read(lastSize, MessageFormatUtilTest.MSG_LEN);
        Assert.assertEquals(baseOffset + lastSize, MessageFormatUtil.getCommitLogOffset(msg1));

        ByteBuffer msg2 = segment.read(lastSize + MessageFormatUtilTest.MSG_LEN, MessageFormatUtilTest.MSG_LEN);
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN, MessageFormatUtil.getCommitLogOffset(msg2));

        ByteBuffer msg3 = segment.read(lastSize + MessageFormatUtilTest.MSG_LEN * 2, MessageFormatUtilTest.MSG_LEN);
        Assert.assertEquals(baseOffset + lastSize + MessageFormatUtilTest.MSG_LEN * 2, MessageFormatUtil.getCommitLogOffset(msg3));
    }
}

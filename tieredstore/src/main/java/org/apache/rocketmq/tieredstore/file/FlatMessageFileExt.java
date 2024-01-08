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

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.metadata.entity.QueueMetadata;
import org.apache.rocketmq.tieredstore.metadata.entity.TopicMetadata;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class FlatMessageFileExt extends FlatMessageFile {

    private final MessageQueue messageQueue;
    private final TopicMetadata topicMetadata;
    private final QueueMetadata queueMetadata;

    public FlatMessageFileExt(FlatFileFactory fileQueueFactory, MessageQueue messageQueue) {

        super(fileQueueFactory, TieredStoreUtil.toPath(messageQueue));
        this.messageQueue = messageQueue;
        this.topicMetadata = this.recoverTopicMetadata();
        this.queueMetadata = this.recoverQueueMetadata();
    }

    @Override
    public void initOffset(long offset) {
        super.initOffset(offset);
        this.flushMetadata();
    }

    public TopicMetadata recoverTopicMetadata() {
        TopicMetadata topicMetadata = this.metadataStore.getTopic(messageQueue.getTopic());
        if (topicMetadata == null) {
            topicMetadata = this.metadataStore.addTopic(messageQueue.getTopic(), -1L);
        }
        return topicMetadata;
    }

    public QueueMetadata recoverQueueMetadata() {
        QueueMetadata queueMetadata = this.metadataStore.getQueue(messageQueue);
        if (queueMetadata == null) {
            queueMetadata = this.metadataStore.addQueue(messageQueue, -1L);
        }
        if (queueMetadata.getMaxOffset() < queueMetadata.getMinOffset()) {
            queueMetadata.setMaxOffset(queueMetadata.getMinOffset());
        }
        return queueMetadata;
    }

    public void flushMetadata() {
        queueMetadata.setMinOffset(super.getConsumeQueueMinOffset());
        queueMetadata.setMaxOffset(super.getConsumeQueueCommitOffset());
        queueMetadata.setUpdateTimestamp(System.currentTimeMillis());
        metadataStore.updateQueue(queueMetadata);
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public TopicMetadata getTopicMetadata() {
        return topicMetadata;
    }

    public QueueMetadata getQueueMetadata() {
        return queueMetadata;
    }

    @Override
    public void shutdown() {
        super.shutdown();
        this.flushMetadata();
    }

    @Override
    public void destroy() {
        super.destroy();
        metadataStore.deleteQueue(messageQueue);
    }
}

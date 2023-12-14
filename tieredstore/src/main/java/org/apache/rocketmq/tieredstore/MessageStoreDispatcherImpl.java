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

import io.opentelemetry.api.common.Attributes;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.file.CompositeQueueFlatFile;
import org.apache.rocketmq.tieredstore.file.TieredFlatFileManager;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsManager;
import org.apache.rocketmq.tieredstore.provider.TieredStoreTopicBlackListFilter;
import org.apache.rocketmq.tieredstore.provider.TieredStoreTopicFilter;
import org.apache.rocketmq.tieredstore.util.MessageBufferUtil;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class MessageStoreDispatcherImpl implements MessageStoreDispatcher {

    protected static final Logger log = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    protected static final long OFFSET_NOT_EXIST = -1L;
    protected volatile boolean stopped = true;
    protected TieredStoreTopicFilter topicFilter;
    protected final String brokerName;
    protected final Semaphore semaphore;
    protected final MessageStore defaultStore;
    protected final TieredMessageStore messageStore;
    protected final TieredMessageStoreConfig storeConfig;
    protected final TieredFlatFileManager flatFileManager;

    public MessageStoreDispatcherImpl(TieredMessageStore messageStore) {
        this.messageStore = messageStore;
        this.defaultStore = messageStore.getMessageStore();
        this.storeConfig = messageStore.getStoreConfig();
        this.brokerName = storeConfig.getBrokerName();
        this.semaphore = new Semaphore(TieredStoreExecutor.QUEUE_CAPACITY / 4);
        this.topicFilter = new TieredStoreTopicBlackListFilter();
        this.flatFileManager = messageStore.getFlatFileManager();
        this.initScheduledTasks();
    }

    public void start() {
        this.stopped = false;
        log.info("MessageStoreDispatcherImpl#start success");
    }

    public void initScheduledTasks() {
        TieredStoreExecutor.commonScheduledExecutor.scheduleWithFixedDelay(
            this::submitAll, 0, 30, TimeUnit.MILLISECONDS);

        TieredStoreExecutor.commonScheduledExecutor.scheduleWithFixedDelay(
            this::cleanExpiredFile, 0, 30, TimeUnit.MILLISECONDS);
    }

    public void submitAll() {
        if (stopped) {
            return;
        }
        try {
            for (CompositeQueueFlatFile flatFile : flatFileManager.deepCopyFlatFileToList()) {
                semaphore.acquire();
                TieredStoreExecutor.commitExecutor.submit(() -> {
                    try {
                        dispatchFlatFile(flatFile);
                    } finally {
                        semaphore.release();
                    }
                });
            }
        } catch (Throwable e) {
            log.error("StoreDispatcher, failed to record submit task", e);
        }
    }

    public void cleanExpiredFile() {
        if (stopped) {
            return;
        }
        try {
            for (CompositeQueueFlatFile flatFile : flatFileManager.deepCopyFlatFileToList()) {
                semaphore.acquire();
                TieredStoreExecutor.commitExecutor.submit(() -> {
                    try {
                        // dispatchFlatFile(flatFile);
                    } finally {
                        semaphore.release();
                    }
                });
            }
        } catch (Throwable e) {
            log.error("StoreDispatcher, failed to record submit task", e);
        }
    }

    public TieredStoreTopicFilter getTopicFilter() {
        return topicFilter;
    }

    public void setTopicFilter(TieredStoreTopicFilter topicFilter) {
        this.topicFilter = topicFilter;
    }

    protected void detectDispatchBehindBytes(String topic, int queueId, long currentOffset) {
        long behindSize = messageStore.getMaxPhyOffset() - currentOffset;
        if (behindSize > storeConfig.getTieredStoreMaxFallBehindSize()) {
            log.warn("MessageStoreDispatcherImpl#detectFallBehind: fall behind too much, " +
                    "topic: {}, queueId: {}, currentOffset: {}, maxFallBehindSize: {}",
                topic, queueId, currentOffset, storeConfig.getTieredStoreMaxFallBehindSize());
        }
        if (behindSize > storeConfig.getTieredStoreMaxFallBehindSize() * 5) {
            log.error("MessageStoreDispatcherImpl#detectFallBehind: fall behind too much, " +
                    "topic: {}, queueId: {}, currentOffset: {}, maxFallBehindSize: {}",
                topic, queueId, currentOffset, storeConfig.getTieredStoreMaxFallBehindSize());
        }
    }

    @Override
    public void dispatchFlatFile(CompositeQueueFlatFile flatFile) {
        if (stopped) {
            return;
        }

        String topic = flatFile.getMessageQueue().getTopic();
        int queueId = flatFile.getMessageQueue().getQueueId();

        if (topicFilter != null && topicFilter.filterTopic(topic)) {
            return;
        }

        long currentOffset = flatFile.getDispatchOffset();
        long minOffsetInQueue = defaultStore.getMinOffsetInQueue(topic, queueId);
        long maxOffsetInQueue = defaultStore.getMaxOffsetInQueue(topic, queueId);

        if (currentOffset == OFFSET_NOT_EXIST) {
            flatFile.initOffset(maxOffsetInQueue);
            currentOffset = maxOffsetInQueue;
        }

        // If the tiered storage feature is turned off midway,
        // it may cause cq discontinuity, resulting in data loss here.
        if (currentOffset < minOffsetInQueue) {
            log.warn("MessageStoreDispatcherImpl#dispatchFlatFile: dispatch offset is too small, " +
                    "topic: {}, queueId: {}, dispatch offset: {}, local cq offset range {}-{}",
                topic, queueId, currentOffset, minOffsetInQueue, maxOffsetInQueue);

            // when dispatch offset is smaller than min offset in local cq
            // some earliest messages may be lost at this time
            flatFileManager.destroyFile(flatFile.getMessageQueue());
            flatFileManager.getOrCreateFlatFileIfAbsent(new MessageQueue(topic, brokerName, queueId))
                .initOffset(maxOffsetInQueue);
            return;
        }

        // Perhaps it was caused by local cq file corruption or ha truncation
        if (currentOffset >= maxOffsetInQueue) {
            return;
        }

        // Flow control by max count, also we could do flow control based on message size
        long groupCommitCount = storeConfig.getTieredStoreGroupCommitCount();
        long groupCommitSize = storeConfig.getTieredStoreGroupCommitSize();
        long targetOffset = Math.min(currentOffset + groupCommitCount, maxOffsetInQueue);

        log.debug("MessageStoreDispatcherImpl#dispatchFlatFile, batch dispatch message, " +
                "topic={}, queueId={}, cq range={}-{}, dispatch offset={}-{}",
            topic, queueId, minOffsetInQueue, maxOffsetInQueue, currentOffset, targetOffset - 1);

        ConsumeQueueInterface consumeQueue = defaultStore.getConsumeQueue(topic, queueId);
        this.detectDispatchBehindBytes(topic, queueId, messageStore.getMaxPhyOffset());

        long dispatchOffset = currentOffset;
        List<DispatchRequest> requestList = new ArrayList<>();
        for (; dispatchOffset < targetOffset; dispatchOffset++) {
            CqUnit cqUnit = consumeQueue.get(dispatchOffset);
            if (cqUnit == null) {
                log.error("[Bug] MessageStoreDispatcherImpl#dispatchFlatFile: cq item is null, " +
                        "topic: {}, queueId: {}, dispatch offset: {}, local cq offset range {}-{}",
                    topic, queueId, currentOffset, minOffsetInQueue, maxOffsetInQueue);
                return;
            }

            SelectMappedBufferResult message =
                defaultStore.selectOneMessageByOffset(cqUnit.getPos(), cqUnit.getSize());
            if (message == null) {
                log.error("[Bug] MessageStoreDispatcherImpl#dispatchFlatFile: message is null, " +
                        "topic: {}, queueId: {}, dispatch offset: {}, local cq offset range {}-{}",
                    topic, queueId, currentOffset, minOffsetInQueue, maxOffsetInQueue);
                return;
            }

            ByteBuffer byteBuffer = message.getByteBuffer();
            AppendResult result = flatFile.appendCommitLog(byteBuffer);
            if (AppendResult.SUCCESS.equals(result)) {
                long newCommitLogOffset = flatFile.getCommitLogMaxOffset() - byteBuffer.remaining();
                Map<String, String> properties = MessageBufferUtil.getProperties(byteBuffer);
                DispatchRequest dispatchRequest = new DispatchRequest(topic, queueId, newCommitLogOffset,
                    cqUnit.getSize(), cqUnit.getTagsCode(), MessageBufferUtil.getStoreTimeStamp(byteBuffer),
                    cqUnit.getQueueOffset(), properties.getOrDefault(MessageConst.PROPERTY_KEYS, ""),
                    properties.getOrDefault(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, ""),
                    0, 0, new HashMap<>());
                dispatchRequest.setOffsetId(MessageBufferUtil.getOffsetId(byteBuffer));
                requestList.add(dispatchRequest);
            } else {
                break;
            }
        }

        flatFile.commitCommitLog();
        for (DispatchRequest dispatchRequest : requestList) {
            flatFile.appendConsumeQueue(dispatchRequest);
        }
        flatFile.commitConsumeQueue();

        Attributes attributes = TieredStoreMetricsManager.newAttributesBuilder()
            .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
            .put(TieredStoreMetricsConstant.LABEL_QUEUE_ID, queueId)
            .put(TieredStoreMetricsConstant.LABEL_FILE_TYPE, FileSegmentType.COMMIT_LOG.name().toLowerCase())
            .build();
        TieredStoreMetricsManager.messagesDispatchTotal.add(dispatchOffset - currentOffset, attributes);
    }

    @Override
    public void deleteExpiredFile(CompositeQueueFlatFile flatFile) {

    }

    public void shutdown() {
        stopped = true;
    }
}

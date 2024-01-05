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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.file.CompositeFlatFileExt;
import org.apache.rocketmq.tieredstore.file.TieredFlatFileManager;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsConstant;
import org.apache.rocketmq.tieredstore.metrics.TieredStoreMetricsManager;
import org.apache.rocketmq.tieredstore.provider.TieredStoreTopicBlackListFilter;
import org.apache.rocketmq.tieredstore.provider.TieredStoreTopicFilter;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class MessageStoreDispatcherImpl extends ServiceThread implements MessageStoreDispatcher {

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

    protected final ReentrantLock dispatchLock;
    protected ConcurrentMap<CompositeFlatFileExt, List<DispatchRequest>> dispatchRequestReadMap;
    protected ConcurrentMap<CompositeFlatFileExt, List<DispatchRequest>> dispatchRequestWriteMap;

    public MessageStoreDispatcherImpl(TieredMessageStore messageStore) {
        this.messageStore = messageStore;
        this.defaultStore = messageStore.getMessageStore();
        this.storeConfig = messageStore.getStoreConfig();
        this.brokerName = storeConfig.getBrokerName();
        this.dispatchLock = new ReentrantLock();
        this.dispatchRequestReadMap = new ConcurrentHashMap<>();
        this.dispatchRequestWriteMap = new ConcurrentHashMap<>();
        this.semaphore = new Semaphore(TieredStoreExecutor.QUEUE_CAPACITY / 4);
        this.topicFilter = new TieredStoreTopicBlackListFilter();
        this.flatFileManager = messageStore.getFlatFileManager();
        this.initScheduledTasks();
    }

    @Override
    public String getServiceName() {
        return "MessageStoreDispatcher";
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
            for (CompositeFlatFileExt flatFile : flatFileManager.deepCopyFlatFileToList()) {
                semaphore.acquire();
                TieredStoreExecutor.commitExecutor.submit(() -> {
                    try {
                        while (true) {
                            if (!dispatchFlatFile(flatFile)) {
                                TimeUnit.MILLISECONDS.sleep(1);
                                break;
                            }
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
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
            for (CompositeFlatFileExt flatFile : flatFileManager.deepCopyFlatFileToList()) {
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

    @Override
    public void dispatch(DispatchRequest request) {
        if (stopped || topicFilter != null && topicFilter.filterTopic(request.getTopic())) {
            return;
        }
        flatFileManager.getOrCreateFlatFileIfAbsent(
            new MessageQueue(request.getTopic(), brokerName, request.getQueueId()));
    }

    @Override
    public boolean dispatchFlatFile(CompositeFlatFileExt flatFile) {
        if (stopped) {
            return false;
        }

        String topic = flatFile.getMessageQueue().getTopic();
        int queueId = flatFile.getMessageQueue().getQueueId();

        if (topicFilter != null && topicFilter.filterTopic(topic)) {
            return false;
        }

        if (flatFile.getFileLock().tryLock()) {
            return false;
        }

        try {
            return dispatch(flatFile, topic, queueId);
        } finally {
            flatFile.getFileLock().unlock();
        }
    }

    public boolean dispatch(CompositeFlatFileExt flatFile, String topic, int queueId) {

        long currentOffset = flatFile.getDispatchOffset();
        long minOffsetInQueue = defaultStore.getMinOffsetInQueue(topic, queueId);
        long maxOffsetInQueue = defaultStore.getMaxOffsetInQueue(topic, queueId);

        if (currentOffset == OFFSET_NOT_EXIST) {
            flatFile.initOffset(maxOffsetInQueue);
            currentOffset = maxOffsetInQueue;
        }

        if (currentOffset < minOffsetInQueue) {
            log.warn("MessageStoreDispatcherImpl#dispatch: dispatch offset is too small, " +
                    "topic: {}, queueId: {}, dispatch offset: {}, local cq offset range {}-{}",
                topic, queueId, currentOffset, minOffsetInQueue, maxOffsetInQueue);

            // Some earliest messages maybe lost at this time
            flatFileManager.destroyFile(flatFile.getMessageQueue());
            flatFileManager.getOrCreateFlatFileIfAbsent(new MessageQueue(topic, brokerName, queueId));
            return true;
        }

        if (flatFile.getCommitLogCommitOffset() != flatFile.getCommitLogMaxOffset()) {
            flatFile.commitCommitLog();
            return true;
        }

        if (flatFile.getConsumeQueueCommitOffset() != flatFile.getConsumeQueueMaxOffset()) {
            flatFile.commitConsumeQueue();
            return true;
        }

        if (currentOffset >= maxOffsetInQueue) {
            return false;
        }

        long totalBufferSize = 0L;
        long groupCommitSize = storeConfig.getTieredStoreGroupCommitSize();
        long groupCommitCount = storeConfig.getTieredStoreGroupCommitCount();
        long targetOffset = Math.min(currentOffset + groupCommitCount, maxOffsetInQueue);

        List<DispatchRequest> requestList = new ArrayList<>();
        ConsumeQueueInterface consumeQueue = defaultStore.getConsumeQueue(topic, queueId);

        long dispatchOffset = currentOffset;
        for (; dispatchOffset < targetOffset; dispatchOffset++) {
            CqUnit cqUnit = consumeQueue.get(dispatchOffset);
            if (cqUnit == null) {
                log.error("[Bug] MessageStoreDispatcherImpl#dispatch: cq item is null, " +
                        "topic: {}, queueId: {}, dispatch offset: {}, local cq offset range {}-{}",
                    topic, queueId, currentOffset, minOffsetInQueue, maxOffsetInQueue);
                continue;
            }

            totalBufferSize += cqUnit.getSize();
            if (totalBufferSize >= groupCommitSize) {
                break;
            }

            SelectMappedBufferResult message =
                defaultStore.selectOneMessageByOffset(cqUnit.getPos(), cqUnit.getSize());
            if (message == null) {
                log.error("[Bug] MessageStoreDispatcherImpl#dispatch: message is null, " +
                        "topic: {}, queueId: {}, dispatch offset: {}, local cq offset range {}-{}",
                    topic, queueId, currentOffset, minOffsetInQueue, maxOffsetInQueue);
                continue;
            }

            ByteBuffer byteBuffer = message.getByteBuffer();
            AppendResult result = flatFile.appendCommitLog(byteBuffer);
            if (!AppendResult.SUCCESS.equals(result)) {
                break;
            }

            long mappedCommitLogOffset = flatFile.getCommitLogMaxOffset() - byteBuffer.remaining();
            Map<String, String> properties = MessageFormatUtil.getProperties(byteBuffer);
            DispatchRequest dispatchRequest = new DispatchRequest(topic, queueId, mappedCommitLogOffset,
                cqUnit.getSize(), cqUnit.getTagsCode(), MessageFormatUtil.getStoreTimeStamp(byteBuffer),
                cqUnit.getQueueOffset(), properties.getOrDefault(MessageConst.PROPERTY_KEYS, ""),
                properties.getOrDefault(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, ""),
                0, 0, new HashMap<>());
            dispatchRequest.setOffsetId(MessageFormatUtil.getOffsetId(byteBuffer));

            result = flatFile.appendConsumeQueue(dispatchRequest);
            if (!AppendResult.SUCCESS.equals(result)) {
                break;
            }
            requestList.add(dispatchRequest);
        }

        flatFile.commitCommitLog();
        if (flatFile.getCommitLogCommitOffset() != flatFile.getCommitLogMaxOffset()) {
            return true;
        }

        flatFile.commitConsumeQueue();
        if (flatFile.getConsumeQueueCommitOffset() != flatFile.getConsumeQueueMaxOffset()) {
            return true;
        }

        this.buildIndex(flatFile, requestList);

        Attributes attributes = TieredStoreMetricsManager.newAttributesBuilder()
            .put(TieredStoreMetricsConstant.LABEL_TOPIC, topic)
            .put(TieredStoreMetricsConstant.LABEL_QUEUE_ID, queueId)
            .put(TieredStoreMetricsConstant.LABEL_FILE_TYPE, FileSegmentType.COMMIT_LOG.name().toLowerCase())
            .build();
        TieredStoreMetricsManager.messagesDispatchTotal.add(dispatchOffset - currentOffset, attributes);

        return false;
    }

    public void buildIndex(CompositeFlatFileExt flatFile, List<DispatchRequest> requestList) {
        dispatchLock.lock();
        try {
            dispatchRequestWriteMap.computeIfAbsent(flatFile, k -> new ArrayList<>()).addAll(requestList);
        } finally {
            dispatchLock.unlock();
        }
    }

    public void swapDispatchRequestList() {
        dispatchLock.lock();
        try {
            dispatchRequestReadMap = dispatchRequestWriteMap;
            dispatchRequestWriteMap = new ConcurrentHashMap<>();
        } finally {
            dispatchLock.unlock();
        }
    }

    @Override
    public void deleteExpiredFile(CompositeFlatFileExt flatFile) {

    }

    public AppendResult appendIndexFile(CompositeFlatFileExt flatFile, DispatchRequest request) {
        if (stopped) {
            return AppendResult.FILE_CLOSED;
        }

        Set<String> keySet = new HashSet<>(
            Arrays.asList(request.getKeys().split(MessageConst.KEY_SEPARATOR)));
        if (StringUtils.isNotBlank(request.getUniqKey())) {
            keySet.add(request.getUniqKey());
        }

        return flatFileManager.getIndexService().putKey(
            request.getTopic(), (int) flatFile.getTopicMetadata().getTopicId(), request.getQueueId(), keySet,
            request.getCommitLogOffset(), request.getMsgSize(), request.getStoreTimestamp());
    }

    protected void buildIndexFile() {
        this.swapDispatchRequestList();

        if (storeConfig.isMessageIndexEnable()) {
            this.dispatchRequestReadMap.clear();
            return;
        }

        for (Map.Entry<CompositeFlatFileExt, List<DispatchRequest>> entry : dispatchRequestReadMap.entrySet()) {
            CompositeFlatFileExt flatFile = entry.getKey();
            List<DispatchRequest> requestList = entry.getValue();
            if (flatFile.isClosed()) {
                requestList.clear();
            }
            for (DispatchRequest request : requestList) {
                this.appendIndexFile(flatFile, request);
            }
        }
        this.dispatchRequestReadMap.clear();
    }

    @Override
    public void run() {
        while (!stopped) {
            waitForRunning(1000);
            buildIndexFile();
        }
    }
}

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
package org.apache.rocketmq.tieredstore.core;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.Pair;
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
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.MessageStoreExecutor;
import org.apache.rocketmq.tieredstore.RemoteMessageStore;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.file.FlatFileStore;
import org.apache.rocketmq.tieredstore.file.FlatMessageFileExt;
import org.apache.rocketmq.tieredstore.index.IndexService;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;

import static org.apache.rocketmq.tieredstore.file.FlatAppendFile.OFFSET_NOT_EXIST;

public class MessageStoreDispatcherImpl extends ServiceThread implements MessageStoreDispatcher {

    protected static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    protected final String brokerName;
    protected final MessageStore defaultStore;
    protected final MessageStoreConfig storeConfig;
    protected final RemoteMessageStore messageStore;
    protected final FlatFileStore flatFileStore;
    protected final MessageStoreExecutor storeExecutor;
    protected final MessageStoreFilter topicFilter;
    protected final Semaphore semaphore;
    protected final IndexService indexService;
    protected final BlockingQueue<Pair<DispatchRequest, Set<String>>> dispatchBlockingQueue;

    public MessageStoreDispatcherImpl(RemoteMessageStore messageStore) {
        this.messageStore = messageStore;
        this.storeConfig = messageStore.getStoreConfig();
        this.defaultStore = messageStore.getDefaultStore();
        this.brokerName = storeConfig.getBrokerName();
        this.semaphore = new Semaphore(
            this.storeConfig.getTieredStoreMaxPendingLimit() / 4);
        this.topicFilter = messageStore.getTopicFilter();
        this.flatFileStore = messageStore.getFlatFileStore();
        this.storeExecutor = messageStore.getStoreExecutor();
        this.indexService = messageStore.getIndexService();
        this.dispatchBlockingQueue = new DisruptorBlockingQueue<>(1024);
    }

    @Override
    public String getServiceName() {
        return "MessageStoreDispatcher";
    }

    @Override
    public void start() {
        super.start();
        this.initScheduledTasks();
        log.info("MessageStoreDispatcherImpl start success");
    }

    public void initScheduledTasks() {
        this.storeExecutor.commonExecutor.scheduleWithFixedDelay(
            this::dispatchAll, 0, 30, TimeUnit.SECONDS);

        this.storeExecutor.commonExecutor.scheduleWithFixedDelay(
            this::deleteExpiredFile, 0, 1, TimeUnit.MINUTES);
    }

    public void dispatchAll() {
        if (stopped) {
            return;
        }

        for (FlatMessageFileExt flatFile : flatFileStore.deepCopyFlatFileToList()) {
            try {
                semaphore.acquire();
                this.dispatch(flatFile)
                    .whenComplete((future, throwable) -> semaphore.release());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void deleteExpiredFile() {
        if (stopped) {
            return;
        }

        for (FlatMessageFileExt flatFile : flatFileStore.deepCopyFlatFileToList()) {
            try {
                semaphore.acquire();
                storeExecutor.bufferCommitExecutor.submit(() -> {
                    try {
                        this.deleteExpiredFile(flatFile);
                    } finally {
                        semaphore.release();
                    }
                });
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void dispatch(DispatchRequest request) {
        if (stopped || topicFilter != null && topicFilter.filterTopic(request.getTopic())) {
            return;
        }
        flatFileStore.getOrCreateFlatFileIfAbsent(
            new MessageQueue(request.getTopic(), brokerName, request.getQueueId()));
    }

    @Override
    public CompletableFuture<Boolean> dispatch(FlatMessageFileExt flatFile) {
        if (stopped) {
            return CompletableFuture.completedFuture(true);
        }

        String topic = flatFile.getMessageQueue().getTopic();
        int queueId = flatFile.getMessageQueue().getQueueId();

        return CompletableFuture.supplyAsync(() ->
            dispatch(flatFile, topic, queueId), storeExecutor.bufferCommitExecutor);
    }

    public boolean dispatch(FlatMessageFileExt flatFile, String topic, int queueId) {

        long currentOffset = flatFile.getConsumeQueueMaxOffset();
        long minOffsetInQueue = defaultStore.getMinOffsetInQueue(topic, queueId);
        long maxOffsetInQueue = defaultStore.getMaxOffsetInQueue(topic, queueId);

        if (currentOffset == OFFSET_NOT_EXIST) {
            // flatFile.(maxOffsetInQueue);
            currentOffset = maxOffsetInQueue;
        }

        if (currentOffset < minOffsetInQueue) {
            // Some earliest messages maybe lost
            log.warn("MessageStoreDispatcherImpl#dispatch: current offset is too small, " +
                    "topic: {}, queueId: {}, current offset: {}, local cq offset range {}-{}",
                topic, queueId, currentOffset, minOffsetInQueue, maxOffsetInQueue);
            flatFileStore.destroyFile(flatFile.getMessageQueue());
            flatFileStore.getOrCreateFlatFileIfAbsent(new MessageQueue(topic, brokerName, queueId));
            return true;
        }

        if (currentOffset >= maxOffsetInQueue) {
            log.warn("MessageStoreDispatcherImpl#dispatch: current offset is too large, " +
                    "topic: {}, queueId: {}, current offset: {}, local cq offset range {}-{}",
                topic, queueId, currentOffset, minOffsetInQueue, maxOffsetInQueue);
            return false;
        }

        long bufferSize = 0L;
        long groupCommitSize = storeConfig.getTieredStoreGroupCommitSize();
        long groupCommitCount = storeConfig.getTieredStoreGroupCommitCount();
        long targetOffset = Math.min(currentOffset + groupCommitCount, maxOffsetInQueue);

        List<DispatchRequest> requestList = new ArrayList<>();
        ConsumeQueueInterface consumeQueue = defaultStore.getConsumeQueue(topic, queueId);

        long offset = currentOffset;
        for (; offset < targetOffset; offset++) {
            CqUnit cqUnit = consumeQueue.get(offset);
            if (cqUnit == null) {
                log.error("[Bug] MessageStoreDispatcherImpl#dispatch: cq item is null, " +
                        "topic: {}, queueId: {}, local cq offset range {}-{}, dispatch offset: {}",
                    topic, queueId, minOffsetInQueue, maxOffsetInQueue, currentOffset);
                continue;
            }

            bufferSize += cqUnit.getSize();
            if (bufferSize >= groupCommitSize) {
                break;
            }

            SelectMappedBufferResult message =
                defaultStore.selectOneMessageByOffset(cqUnit.getPos(), cqUnit.getSize());
            if (message == null) {
                log.error("[Bug] MessageStoreDispatcherImpl#dispatch: message is null, " +
                        "topic: {}, queueId: {}, local cq offset range {}-{}, dispatch offset: {}",
                    topic, queueId, minOffsetInQueue, maxOffsetInQueue, currentOffset);
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

        flatFile.commitAsync().thenApply(result -> {
            // not retry immediately
            if (!result) {
                return false;
            }
            this.doBuildIndex(flatFile, requestList);
            return true;
        });

//        Attributes attributes = MessageStoreMetricsManager.newAttributesBuilder()
//            .put(MessageStoreMetricsConstant.LABEL_TOPIC, topic)
//            .put(MessageStoreMetricsConstant.LABEL_QUEUE_ID, queueId)
//            .put(MessageStoreMetricsConstant.LABEL_FILE_TYPE, FileSegmentType.COMMIT_LOG.name().toLowerCase())
//            .build();
//        MessageStoreMetricsManager.messagesDispatchTotal.add(offset - currentOffset, attributes);

        return false;
    }

    @Override
    public CompletableFuture<Boolean> deleteExpiredFile(FlatMessageFileExt flatFile) {
        return null;
    }

    public void doBuildIndex(FlatMessageFileExt flatFile, List<DispatchRequest> requestList) {
        List<Pair<DispatchRequest, Set<String>>> result = new ArrayList<>(requestList.size());
        for (DispatchRequest request : requestList) {
            Set<String> keySet = new HashSet<>(
                Arrays.asList(request.getKeys().split(MessageConst.KEY_SEPARATOR)));
            if (StringUtils.isNotBlank(request.getUniqKey())) {
                keySet.add(request.getUniqKey());
            }
            result.add(new Pair<>(request, keySet));
        }
        dispatchBlockingQueue.addAll(result);
    }

    @Override
    public void run() {
        while (!stopped) {
            waitForRunning(1000);
            while (true) {
                try {
                    Pair<DispatchRequest, Set<String>> pair =
                        dispatchBlockingQueue.poll(10, TimeUnit.MILLISECONDS);
                    if (pair == null) {
                        break;
                    }
                    DispatchRequest request = pair.getObject1();
                    Set<String> keySet = pair.getObject2();
                    indexService.putKey(request.getTopic(), 0, request.getQueueId(), keySet,
                        request.getCommitLogOffset(), request.getMsgSize(), request.getStoreTimestamp());
                } catch (Throwable e) {
                    log.error("MessageStoreDispatcherImpl#run: build index error", e);
                }
            }
        }
    }
}

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

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.index.IndexService;
import org.apache.rocketmq.tieredstore.index.IndexStoreService;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class TieredFlatFileManager {

    private static final Logger log = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private final TieredMetadataStore metadataStore;
    private final TieredMessageStoreConfig storeConfig;
    private final IndexStoreService indexStoreService;
    private final TieredFileAllocator fileAllocator;
    private final ConcurrentMap<MessageQueue, CompositeFlatFileExt> flatFileConcurrentMap;

    public TieredFlatFileManager(TieredMetadataStore metadataStore, TieredMessageStoreConfig storeConfig)
        throws ClassNotFoundException, NoSuchMethodException {

        this.storeConfig = storeConfig;
        this.metadataStore = metadataStore;
        this.fileAllocator = new TieredFileAllocator(metadataStore, storeConfig);
        this.indexStoreService = new IndexStoreService(fileAllocator, TieredStoreUtil.toPath(new MessageQueue(
            TieredStoreUtil.RMQ_SYS_TIERED_STORE_INDEX_TOPIC, storeConfig.getBrokerName(), 0)));
        this.flatFileConcurrentMap = new ConcurrentHashMap<>();
    }

    public boolean load() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            this.flatFileConcurrentMap.clear();
            this.recoverSequenceNumber();
            this.recoverTieredFlatFile();
            log.info("Message store recover end, total cost={}ms", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            long costTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            log.info("Message store recover error, total cost={}ms", costTime);
            LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME)
                .error("Message store recover error, total cost={}ms", costTime, e);
            return false;
        }
        return true;
    }

    public void recoverSequenceNumber() {
        AtomicLong topicSequenceNumber = new AtomicLong();
        metadataStore.iterateTopic(topicMetadata -> {
            if (topicMetadata != null && topicMetadata.getTopicId() > 0) {
                topicSequenceNumber.set(Math.max(topicSequenceNumber.get(), topicMetadata.getTopicId()));
            }
        });
        metadataStore.setTopicSequenceNumber(topicSequenceNumber.incrementAndGet());
    }

    public void recoverTieredFlatFile() {
        Semaphore semaphore = new Semaphore((int) (TieredStoreExecutor.QUEUE_CAPACITY * 0.75));
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        metadataStore.iterateTopic(topicMetadata -> {
            try {
                semaphore.acquire();
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    try {
                        Stopwatch subWatch = Stopwatch.createStarted();
                        if (topicMetadata.getStatus() != 0) {
                            return;
                        }
                        AtomicLong queueCount = new AtomicLong();
                        metadataStore.iterateQueue(topicMetadata.getTopic(), queueMetadata -> {
                            this.getOrCreateFlatFileIfAbsent(new MessageQueue(topicMetadata.getTopic(),
                                storeConfig.getBrokerName(), queueMetadata.getQueue().getQueueId()));
                            queueCount.incrementAndGet();
                        });

                        if (queueCount.get() == 0L) {
                            metadataStore.deleteTopic(topicMetadata.getTopic());
                        } else {
                            log.info("Recover TopicFlatFile, topic: {}, queueCount: {}, cost: {}ms",
                                topicMetadata.getTopic(), queueCount.get(), subWatch.elapsed(TimeUnit.MILLISECONDS));
                        }
                    } catch (Exception e) {
                        log.error("Recover TopicFlatFile error, topic: {}", topicMetadata.getTopic(), e);
                    } finally {
                        semaphore.release();
                    }
                }, TieredStoreExecutor.commitExecutor);
                futures.add(future);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    public TieredMetadataStore getMetadataStore() {
        return metadataStore;
    }

    public TieredMessageStoreConfig getStoreConfig() {
        return storeConfig;
    }

    public CompositeFlatFileExt getOrCreateFlatFileIfAbsent(MessageQueue messageQueue) {
        return flatFileConcurrentMap.computeIfAbsent(messageQueue,
            mq -> new CompositeFlatFileExt(fileAllocator, mq));
    }

    public CompositeFlatFileExt getFlatFile(MessageQueue messageQueue) {
        return flatFileConcurrentMap.get(messageQueue);
    }

    public ImmutableList<CompositeFlatFileExt> deepCopyFlatFileToList() {
        return ImmutableList.copyOf(flatFileConcurrentMap.values());
    }

    ///**
    // * Building indexes with offsetId is no longer supported because offsetId has changed in tiered storage
    // */
    //public AppendResult appendIndexFile(DispatchRequest request) {
    //    if (closed) {
    //        return AppendResult.FILE_CLOSED;
    //    }
    //
    //    Set<String> keySet = new HashSet<>(
    //        Arrays.asList(request.getKeys().split(MessageConst.KEY_SEPARATOR)));
    //    if (StringUtils.isNotBlank(request.getUniqKey())) {
    //        keySet.add(request.getUniqKey());
    //    }
    //
    //    return indexStoreService.putKey(
    //        messageQueue.getTopic(), (int) topicSequenceNumber, messageQueue.getQueueId(), keySet,
    //        request.getCommitLogOffset(), request.getMsgSize(), request.getStoreTimestamp());
    //}

    public IndexService getIndexService() {
        return this.indexStoreService;
    }

    public void doScheduleCommitTask() {
        TieredStoreExecutor.commonScheduledExecutor.scheduleWithFixedDelay(() -> {
            try {

            } catch (Throwable e) {
                log.error("Commit flat file periodically failed: ", e);
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    public void doScheduleCleanExpiredTask() {
        long expiredTimeStamp = System.currentTimeMillis() -
            TimeUnit.HOURS.toMillis(storeConfig.getTieredStoreFileReservedTime());

        for (CompositeFlatFileExt flatFile : deepCopyFlatFileToList()) {
            TieredStoreExecutor.cleanExpiredFileExecutor.submit(() -> {
                flatFile.cleanExpiredFile(expiredTimeStamp);
                flatFile.destroyExpiredFile();
            });
        }
    }

    public void shutdown() {
        if (indexStoreService != null) {
            indexStoreService.shutdown();
        }
        flatFileConcurrentMap.values().forEach(CompositeFlatFile::shutdown);
    }

    public void destroyFile(MessageQueue mq) {
        if (mq == null) {
            return;
        }

        CompositeFlatFileExt flatFile = flatFileConcurrentMap.remove(mq);
        if (flatFile != null) {
            flatFile.shutdown();
            flatFile.destroy();
        }
    }

    public void destroy() {
        if (indexStoreService != null) {
            indexStoreService.destroy();
        }
        flatFileConcurrentMap.values().forEach(CompositeFlatFile::destroy);
        flatFileConcurrentMap.clear();
    }
}

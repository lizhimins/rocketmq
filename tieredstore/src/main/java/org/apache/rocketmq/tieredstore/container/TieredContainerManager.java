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
package org.apache.rocketmq.tieredstore.container;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.TieredStoreExecutor;
import org.apache.rocketmq.tieredstore.metadata.TieredMetadataStore;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class TieredContainerManager {

    private static final Logger logger = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private static volatile TieredContainerManager instance;
    private static volatile TieredIndexFile indexFile;

    private final TieredMetadataStore metadataStore;
    private final TieredMessageStoreConfig storeConfig;
    private final TieredFileFactory tieredFileFactory;
    private final ConcurrentMap<MessageQueue, TieredFileChunkWithQueue> messageQueueContainerMap;

    public TieredContainerManager(TieredMessageStoreConfig storeConfig)
        throws ClassNotFoundException, NoSuchMethodException {

        this.storeConfig = storeConfig;
        this.metadataStore = TieredStoreUtil.getMetadataStore(storeConfig);
        this.tieredFileFactory = new TieredFileFactory(storeConfig);
        this.messageQueueContainerMap = new ConcurrentHashMap<>();
        this.doScheduleTask();
    }


    public static TieredContainerManager getInstance(TieredMessageStoreConfig storeConfig) {
        if (storeConfig == null) {
            return instance;
        }

        if (instance == null) {
            synchronized (TieredContainerManager.class) {
                if (instance == null) {
                    try {
                        instance = new TieredContainerManager(storeConfig);
                    } catch (Exception e) {
                        logger.error("TieredContainerManager#getInstance: create container manager failed", e);
                    }
                }
            }
        }
        return instance;
    }

    public static TieredIndexFile getIndexFile(TieredMessageStoreConfig storeConfig) {
        if (storeConfig == null) {
            return indexFile;
        }

        if (indexFile == null) {
            synchronized (TieredContainerManager.class) {
                if (indexFile == null) {
                    try {
                        String filePath = TieredStoreUtil.toPath(new MessageQueue(
                            TieredStoreUtil.RMQ_SYS_TIERED_STORE_INDEX_TOPIC, storeConfig.getBrokerName(), 0));
                        indexFile = new TieredIndexFile(new TieredFileFactory(storeConfig), filePath);
                    } catch (Exception e) {
                        logger.error("TieredContainerManager#getIndexFile: create index file failed", e);
                    }
                }
            }
        }
        return indexFile;
    }

    private void doScheduleTask() {
        TieredStoreExecutor.COMMON_SCHEDULED_EXECUTOR.scheduleWithFixedDelay(() -> {
            try {
                Random random = new Random();
                for (TieredFileChunkWithQueue container : getAllMQContainer()) {
                    int delay = random.nextInt(storeConfig.getMaxCommitJitter());
                    TieredStoreExecutor.COMMIT_EXECUTOR.schedule(() -> {
                        try {
                            container.commitCommitLog();
                        } catch (Throwable e) {
                            MessageQueue mq = container.getMessageQueue();
                            logger.error("commit commitLog periodically failed: topic: {}, queue: {}", mq.getTopic(), mq.getQueueId(), e);
                        }
                    }, delay, TimeUnit.MILLISECONDS);
                    TieredStoreExecutor.COMMIT_EXECUTOR.schedule(() -> {
                        try {
                            container.commitConsumeQueue();
                        } catch (Throwable e) {
                            MessageQueue mq = container.getMessageQueue();
                            logger.error("commit consumeQueue periodically failed: topic: {}, queue: {}", mq.getTopic(), mq.getQueueId(), e);
                        }
                    }, delay, TimeUnit.MILLISECONDS);
                }
                TieredStoreExecutor.COMMIT_EXECUTOR.schedule(() -> {
                    try {
                        if (indexFile != null) {
                            indexFile.commit(true);
                        }
                    } catch (Throwable e) {
                        logger.error("commit indexFile periodically failed", e);
                    }
                }, 0, TimeUnit.MILLISECONDS);
            } catch (Throwable e) {
                logger.error("commit container periodically failed: ", e);
            }
        }, 60, 60, TimeUnit.SECONDS);

        TieredStoreExecutor.COMMON_SCHEDULED_EXECUTOR.scheduleWithFixedDelay(() -> {
            try {
                long expiredTimeStamp = System.currentTimeMillis() - (long) storeConfig.getTieredStoreFileReservedTime() * 60 * 60 * 1000;
                Random random = new Random();
                for (TieredFileChunkWithQueue container : getAllMQContainer()) {
                    int delay = random.nextInt(storeConfig.getMaxCommitJitter());
                    TieredStoreExecutor.CLEAN_EXPIRED_FILE_EXECUTOR.schedule(() -> {
                        container.getFileChunkLock().lock();
                        try {
                            container.cleanExpiredFile(expiredTimeStamp);
                            container.destroyExpiredFile();
                            if (container.getConsumeQueueBaseOffset() == -1) {
                                destroyContainer(container.getMessageQueue());
                            }
                        } finally {
                            container.getFileChunkLock().unlock();
                        }
                    }, delay, TimeUnit.MILLISECONDS);
                }
                if (indexFile != null) {
                    indexFile.cleanExpiredFile(expiredTimeStamp);
                    indexFile.destroyExpiredFile();
                }
            } catch (Throwable e) {
                logger.error("clean container expired file failed: ", e);
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    public boolean load() {
        try {
            AtomicLong topicSequenceNumber = new AtomicLong();
            List<Future<?>> futureList = new ArrayList<>();
            messageQueueContainerMap.clear();
            metadataStore.iterateTopic(topicMetadata -> {
                topicSequenceNumber.set(Math.max(topicSequenceNumber.get(), topicMetadata.getTopicId()));
                Future<?> future = TieredStoreExecutor.DISPATCH_EXECUTOR.submit(() -> {
                    if (topicMetadata.getStatus() != 0) {
                        return;
                    }
                    try {
                        metadataStore.iterateQueue(topicMetadata.getTopic(),
                            queueMetadata -> getOrCreateMQContainer(
                                new MessageQueue(topicMetadata.getTopic(),
                                    storeConfig.getBrokerName(),
                                    queueMetadata.getQueue().getQueueId())));
                    } catch (Exception e) {
                        logger.error("load mq container from metadata failed", e);
                    }
                });
                futureList.add(future);
            });

            // Wait for load all metadata done
            for (Future<?> future : futureList) {
                future.get();
            }
            metadataStore.setTopicSequenceNumber(topicSequenceNumber.incrementAndGet());
        } catch (Exception e) {
            logger.error("load mq container from metadata failed", e);
            return false;
        }
        return true;
    }

    public void cleanup() {
        messageQueueContainerMap.clear();
        cleanStaticReference();
    }

    private static void cleanStaticReference() {
        instance = null;
        indexFile = null;
    }

    @Nullable
    public TieredFileChunkWithQueue getOrCreateMQContainer(MessageQueue messageQueue) {
        return messageQueueContainerMap.computeIfAbsent(messageQueue, mq -> {
            try {
                logger.info("TieredContainerManager#getOrCreateMQContainer: try to create new container: topic: {}, queueId: {}",
                    messageQueue.getTopic(), messageQueue.getQueueId());
                return new TieredFileChunkWithQueue(tieredFileFactory, mq);
            } catch (Exception e) {
                logger.error("TieredContainerManager#getOrCreateMQContainer: create new container failed: topic: {}, queueId: {}",
                    messageQueue.getTopic(), messageQueue.getQueueId(), e);
                return null;
            }
        });
    }

    @Nullable
    public TieredFileChunkWithQueue getMQContainer(MessageQueue messageQueue) {
        return messageQueueContainerMap.get(messageQueue);
    }

    public ImmutableList<TieredFileChunkWithQueue> getAllMQContainer() {
        return ImmutableList.copyOf(messageQueueContainerMap.values());
    }

    public void shutdown() {
        if (indexFile != null) {
            indexFile.commit(true);
        }
        for (TieredFileChunk container : getAllMQContainer()) {
            container.shutdown();
        }
    }

    public void destroy() {
        if (indexFile != null) {
            indexFile.destroy();
        }
        ImmutableList<TieredFileChunkWithQueue> containerList = getAllMQContainer();
        cleanup();
        for (TieredFileChunk container : containerList) {
            container.destroy();
        }
    }

    public void destroyContainer(MessageQueue mq) {
        TieredFileChunkWithQueue container = messageQueueContainerMap.remove(mq);
        if (container != null) {
            MessageQueue messageQueue = container.getMessageQueue();
            logger.info("BlobContainerManager#destroyContainer: try to destroy container: topic: {}, queueId: {}",
                messageQueue.getTopic(), messageQueue.getQueueId());
            container.destroy();
        }
    }
}

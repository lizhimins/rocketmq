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

import com.github.benmanes.caffeine.cache.Cache;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;

public class FlatMessageFile implements FlatFileInterface {

    protected static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);
    protected static final long OFFSET_NOT_EXIST = -1L;
    protected volatile boolean closed = false;

    protected final String filePath;
    protected final ReentrantLock fileLock;
    protected final MessageStoreConfig storeConfig;
    protected final MetadataStore metadataStore;
    protected final FlatCommitLogFile commitLog;
    protected final FlatConsumeQueueFile consumeQueue;
    protected final AtomicInteger readAheadFactor;
    protected final Cache<String, Long> groupOffsetCache;
    protected final ConcurrentMap<String, CompletableFuture<?>> inFlightRequestMap;

    public FlatMessageFile(FlatFileFactory fileAllocator, String filePath) {

        this.filePath = filePath;
        this.fileLock = new ReentrantLock();
        this.storeConfig = fileAllocator.getStoreConfig();
        this.metadataStore = fileAllocator.getMetadataStore();

        this.commitLog = new FlatCommitLogFile(fileAllocator, filePath);
        this.consumeQueue = new FlatConsumeQueueFile(fileAllocator, filePath);
        this.groupOffsetCache = this.initOffsetCache();
        this.readAheadFactor = new AtomicInteger(this.storeConfig.getReadAheadMinFactor());
        this.inFlightRequestMap = new ConcurrentHashMap<>();
    }

    private Cache<String, Long> initOffsetCache() {
//        return Caffeine.newBuilder()
//            .expireAfterWrite(2, TimeUnit.MINUTES)
//            .removalListener((key, value, cause) -> {
//                if (cause.equals(RemovalCause.EXPIRED)) {
//                    inFlightRequestMap.remove(new InFlightRequestKey((String) key));
//                }
//            }).build();
        return null;
    }

    public boolean isClosed() {
        return closed;
    }

    public ReentrantLock getFileLock() {
        return fileLock;
    }

    @Override
    public void initOffset(long offset) {
//        fileLock.lock();
//        try {
//            if (!consumeQueue.isInitialized()) {
//                consumeQueue.setBaseOffset(offset * MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
//            }
//        } finally {
//            fileLock.unlock();
//        }
    }

    @Override
    public long getCommitLogMinOffset() {
        return commitLog.getMinOffset();
    }

    @Override
    public long getCommitLogMaxOffset() {
        return commitLog.getMaxOffset();
    }

    @Override
    public long getCommitLogCommitOffset() {
        return commitLog.getCommitOffset();
    }

    @Override
    public long getConsumeQueueMinOffset() {
        long cqOffset = consumeQueue.getMinOffset() / MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE;
        long effectiveOffset = this.commitLog.getMinConsumeQueueOffset();
        return Math.max(cqOffset, effectiveOffset);
    }

    @Override
    public long getConsumeQueueMaxOffset() {
        return consumeQueue.getMaxOffset() / MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE;
    }

    @Override
    public long getConsumeQueueCommitOffset() {
        return consumeQueue.getCommitOffset() / MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE;
    }

    @Override
    public CompletableFuture<ByteBuffer> getMessageAsync(long queueOffset) {
//        return getConsumeQueueAsync(queueOffset).thenCompose(cqBuffer -> {
//            long commitLogOffset = CQItemBufferUtil.getCommitLogOffset(cqBuffer);
//            int length = CQItemBufferUtil.getSize(cqBuffer);
//            return getCommitLogAsync(commitLogOffset, length);
//        });
        return null;
    }

    @Override
    public long getOffsetInConsumeQueueByTime(long timestamp, BoundaryType boundaryType) {
        Pair<Long, Long> pair = consumeQueue.getQueueOffsetInFileByTime(timestamp, boundaryType);
        long minQueueOffset = pair.getLeft();
        long maxQueueOffset = pair.getRight();

        if (maxQueueOffset == -1 || maxQueueOffset < minQueueOffset) {
            return -1L;
        }

        long low = minQueueOffset;
        long high = maxQueueOffset;

        long offset = 0;

        // Handle the following corner cases first:
        // 1. store time of (high) < timestamp
        // 2. store time of (low) > timestamp
        long storeTime;
        // Handle case 1
        ByteBuffer message = getMessageAsync(maxQueueOffset).join();
        storeTime = MessageFormatUtil.getStoreTimeStamp(message);
        if (storeTime < timestamp) {
            switch (boundaryType) {
                case LOWER:
                    return maxQueueOffset + 1;
                case UPPER:
                    return maxQueueOffset;
                default:
                    log.warn("CompositeFlatFile#getQueueOffsetByTime: unknown boundary boundaryType");
                    break;
            }
        }

        // Handle case 2
        message = getMessageAsync(minQueueOffset).join();
        storeTime = MessageFormatUtil.getStoreTimeStamp(message);
        if (storeTime > timestamp) {
            switch (boundaryType) {
                case LOWER:
                    return minQueueOffset;
                case UPPER:
                    return 0L;
                default:
                    log.warn("CompositeFlatFile#getQueueOffsetByTime: unknown boundary boundaryType");
                    break;
            }
        }

        // Perform binary search
        long midOffset = -1;
        long targetOffset = -1;
        long leftOffset = -1;
        long rightOffset = -1;
        while (high >= low) {
            midOffset = (low + high) / 2;
            message = getMessageAsync(midOffset).join();
            storeTime = MessageFormatUtil.getStoreTimeStamp(message);
            if (storeTime == timestamp) {
                targetOffset = midOffset;
                break;
            } else if (storeTime > timestamp) {
                high = midOffset - 1;
                rightOffset = midOffset;
            } else {
                low = midOffset + 1;
                leftOffset = midOffset;
            }
        }

        if (targetOffset != -1) {
            // We just found ONE matched record. These next to it might also share the same store-timestamp.
            offset = targetOffset;
            long previousAttempt = targetOffset;
            switch (boundaryType) {
                case LOWER:
                    while (true) {
                        long attempt = previousAttempt - 1;
                        if (attempt < minQueueOffset) {
                            break;
                        }
                        message = getMessageAsync(attempt).join();
                        storeTime = MessageFormatUtil.getStoreTimeStamp(message);
                        if (storeTime == timestamp) {
                            previousAttempt = attempt;
                            continue;
                        }
                        break;
                    }
                    offset = previousAttempt;
                    break;
                case UPPER:
                    while (true) {
                        long attempt = previousAttempt + 1;
                        if (attempt > maxQueueOffset) {
                            break;
                        }

                        message = getMessageAsync(attempt).join();
                        storeTime = MessageFormatUtil.getStoreTimeStamp(message);
                        if (storeTime == timestamp) {
                            previousAttempt = attempt;
                            continue;
                        }
                        break;
                    }
                    offset = previousAttempt;
                    break;
                default:
                    log.warn("CompositeFlatFile#getQueueOffsetByTime: unknown boundary boundaryType");
                    break;
            }
        } else {
            // Given timestamp does not have any message records. But we have a range enclosing the
            // timestamp.
            /*
             * Consider the follow case: t2 has no consume queue entry and we are searching offset of
             * t2 for lower and upper boundaries.
             *  --------------------------
             *   timestamp   Consume Queue
             *       t1          1
             *       t1          2
             *       t1          3
             *       t3          4
             *       t3          5
             *   --------------------------
             * Now, we return 3 as upper boundary of t2 and 4 as its lower boundary. It looks
             * contradictory at first sight, but it does make sense when performing range queries.
             */
            switch (boundaryType) {
                case LOWER: {
                    offset = rightOffset;
                    break;
                }

                case UPPER: {
                    offset = leftOffset;
                    break;
                }
                default: {
                    log.warn("CompositeFlatFile#getQueueOffsetByTime: unknown boundary boundaryType");
                    break;
                }
            }
        }
        return offset;
    }

    @Override
    public AppendResult appendCommitLog(ByteBuffer message) {
        if (closed) {
            return AppendResult.FILE_CLOSED;
        }

//        AppendResult result = commitLog.append(message);
//        if (result == AppendResult.SUCCESS) {
//            dispatchOffset.incrementAndGet();
//        }
//        return result;

        return AppendResult.SUCCESS;
    }

    @Override
    public AppendResult appendConsumeQueue(DispatchRequest request) {
        if (closed) {
            return AppendResult.FILE_CLOSED;
        }

//        if (request.getConsumeQueueOffset() != this.getConsumeQueueMaxOffset()) {
//            return AppendResult.OFFSET_INCORRECT;
//        }

        return consumeQueue.append(request.getCommitLogOffset(),
            request.getMsgSize(), request.getTagsCode(), request.getStoreTimestamp());
    }

    @Override
    public long getDispatchOffset() {
        return 0;
    }

    @Override
    public long getDispatchCommitOffset() {
        return 0;
    }

    @Override
    public CompletableFuture<ByteBuffer> getCommitLogAsync(long offset, int length) {
        return commitLog.readAsync(offset, length);
    }

    @Override
    public CompletableFuture<ByteBuffer> getConsumeQueueAsync(long queueOffset) {
        return getConsumeQueueAsync(queueOffset, 1);
    }

    @Override
    public CompletableFuture<ByteBuffer> getConsumeQueueAsync(long queueOffset, int count) {
        return consumeQueue.readAsync(queueOffset * MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE,
            count * MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
    }

    @Override
    public void commitCommitLog() {
        commitLog.commit(true);
    }

    @Override
    public void commitConsumeQueue() {
        consumeQueue.commit(true);
    }

    public int getReadAheadFactor() {
        return readAheadFactor.get();
    }

    public void increaseReadAheadFactor() {
        readAheadFactor.set(Math.min(readAheadFactor.get() + 1, storeConfig.getReadAheadMaxFactor()));
    }

    public void decreaseReadAheadFactor() {
        readAheadFactor.set(Math.max(readAheadFactor.get() - 1, storeConfig.getReadAheadMinFactor()));
    }

    public void recordGroupAccess(String group, long offset) {
        groupOffsetCache.put(group, offset);
    }

    public long getActiveGroupCount(long minOffset, long maxOffset) {
        return groupOffsetCache.asMap()
            .values()
            .stream()
            .filter(offset -> offset >= minOffset && offset <= maxOffset)
            .count();
    }

    public long getActiveGroupCount() {
        return groupOffsetCache.estimatedSize();
    }

//    public InFlightRequestFuture getInflightRequest(long offset, int batchSize) {
//        Optional<InFlightRequestFuture> optional = inFlightRequestMap.entrySet()
//            .stream()
//            .filter(entry -> {
//                InFlightRequestKey key = entry.getKey();
//                return Math.max(key.getOffset(), offset) <=
//                    Math.min(key.getOffset() + key.getBatchSize(), offset + batchSize);
//            })
//            .max(Comparator.comparing(entry -> entry.getKey().getRequestTime()))
//            .map(Map.Entry::getValue);
//        return optional.orElseGet(() -> new InFlightRequestFuture(Long.MAX_VALUE, new ArrayList<>()));
//    }
//
//    public InFlightRequestFuture getInflightRequest(String group, long offset, int batchSize) {
//        InFlightRequestFuture future = inFlightRequestMap.get(new InFlightRequestKey(group));
//        if (future != null && !future.isAllDone()) {
//            return future;
//        }
//        return getInflightRequest(offset, batchSize);
//    }
//
//    public void putInflightRequest(String group, long offset, int requestMsgCount,
//        List<Pair<Integer, CompletableFuture<Long>>> futureList) {
//        InFlightRequestKey key = new InFlightRequestKey(group, offset, requestMsgCount);
//        inFlightRequestMap.remove(key);
//        inFlightRequestMap.putIfAbsent(key, new InFlightRequestFuture(offset, futureList));
//    }

    @Override
    public int hashCode() {
        return filePath.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        return StringUtils.equals(filePath, ((FlatMessageFile) obj).filePath);
    }

    @Override
    public void cleanExpiredFile(long expireTimestamp) {
        fileLock.lock();
        try {
            if (closed) {
                return;
            }
            commitLog.cleanExpiredFile(expireTimestamp);
            consumeQueue.cleanExpiredFile(expireTimestamp);
        } finally {
            fileLock.unlock();
        }
    }

    @Override
    public void destroyExpiredFile() {
        fileLock.lock();
        try {
            if (closed) {
                return;
            }
            commitLog.destroyExpiredFile();
            consumeQueue.destroyExpiredFile();
        } finally {
            fileLock.unlock();
        }
    }

    public void shutdown() {
        closed = true;
        fileLock.lock();
        try {
            commitLog.commit(true);
            consumeQueue.commit(true);
        } finally {
            fileLock.unlock();
        }
    }

    public void destroy() {
        closed = true;
        fileLock.lock();
        try {
            commitLog.destroy();
            consumeQueue.destroy();
            metadataStore.deleteFileSegment(filePath, FileSegmentType.COMMIT_LOG);
            metadataStore.deleteFileSegment(filePath, FileSegmentType.CONSUME_QUEUE);
        } catch (Exception e) {
            log.error("CompositeFlatFile#destroy: delete file failed, filePath: {}", filePath, e);
        } finally {
            fileLock.unlock();
        }
    }
}

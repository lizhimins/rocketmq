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
import java.util.function.Supplier;
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

    public FlatMessageFile(FlatFileFactory fileFactory, String filePath) {
        this.filePath = filePath;
        this.fileLock = new ReentrantLock();
        this.storeConfig = fileFactory.getStoreConfig();
        this.metadataStore = fileFactory.getMetadataStore();
        this.commitLog = fileFactory.createFlatFileForCommitLog(filePath);
        this.consumeQueue = fileFactory.createFlatFileForConsumeQueue(filePath);
        this.groupOffsetCache = this.initOffsetCache();
        this.readAheadFactor = new AtomicInteger(this.storeConfig.getReadAheadMinFactor());
        this.inFlightRequestMap = new ConcurrentHashMap<>();
    }

    private Cache<String, Long> initOffsetCache() {
        //return Caffeine.newBuilder()
        //    .expireAfterWrite(2, TimeUnit.MINUTES)
        //    .removalListener((key, value, cause) -> {
        //        if (cause.equals(RemovalCause.EXPIRED)) {
        //            inFlightRequestMap.remove(new InFlightRequestKey((String) key));
        //        }
        //    }).build();
        return null;
    }

//    @Override
//    public void initOffset(long offset) {
//        fileLock.lock();
//        try {
//            if (!consumeQueue.isInitialized()) {
//                consumeQueue.setBaseOffset(offset * MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
//            }
//        } finally {
//            fileLock.unlock();
//        }
//    }

    @Override
    public long getConsumeQueueMinOffset() {
        long cqOffset = consumeQueue.getMinOffset() / MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE;
        long effectiveOffset = this.commitLog.getMinConsumeQueueOffset();
        return Math.max(cqOffset, effectiveOffset);
    }

    @Override
    public long getConsumeQueueMaxOffset() {
        return consumeQueue.getAppendOffset() / MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE;
    }

    @Override
    public long getConsumeQueueCommitOffset() {
        return consumeQueue.getCommitOffset() / MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE;
    }

    @Override
    public AppendResult appendCommitLog(ByteBuffer message) {
        if (closed) {
            return AppendResult.FILE_CLOSED;
        }
        return commitLog.append(message, MessageFormatUtil.getStoreTimeStamp(message));
    }

    @Override
    public AppendResult appendConsumeQueue(DispatchRequest request) {
        if (closed) {
            return AppendResult.FILE_CLOSED;
        }
        ByteBuffer buffer = ByteBuffer.allocate(MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
        buffer.putLong(request.getCommitLogOffset());
        buffer.putInt(request.getMsgSize());
        buffer.putLong(request.getTagsCode());
        buffer.flip();
        return consumeQueue.append(buffer, request.getStoreTimestamp());
    }

    @Override
    public CompletableFuture<Void> commitAsync() {
        return CompletableFuture.completedFuture(null);
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
    public CompletableFuture<ByteBuffer> getCommitLogAsync(long offset, int length) {
        return commitLog.readAsync(offset, length);
    }

    @Override
    public CompletableFuture<ByteBuffer> getConsumeQueueAsync(long queueOffset) {
        return this.getConsumeQueueAsync(queueOffset, 1);
    }

    @Override
    public CompletableFuture<ByteBuffer> getConsumeQueueAsync(long queueOffset, int count) {
        return consumeQueue.readAsync(queueOffset * MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE,
            count * MessageFormatUtil.CONSUME_QUEUE_UNIT_SIZE);
    }

    @Override
    public CompletableFuture<Long> getOffsetInQueueByTime(long timestamp, BoundaryType boundaryType) {
        return null;
    }

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

    @Override
    public CompletableFuture<Void> destroyExpiredFile(long timestamp) {
        return null;
    }

    public CompletableFuture<Void> destroy() {
        return CompletableFuture.supplyAsync(() -> {
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
            return null;
        });
    }
}

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

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

import static org.apache.rocketmq.tieredstore.file.CompositeFlatFile.OFFSET_NOT_EXIST;

public class TieredCommitLog {

    private static final Logger log = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    /**
     * item size: int, 4 bytes
     * magic code: int, 4 bytes
     * max store timestamp: long, 8 bytes
     */
    public static final int CODA_SIZE = 4 + 8 + 4;
    public static final int BLANK_MAGIC_CODE = 0xBBCCDDEE ^ 1880681586 + 8;

    private final MessageStoreConfig storeConfig;
    private final TieredFlatFile flatFile;
    private final AtomicLong consumeQueueMinOffset;

    public TieredCommitLog(TieredFileAllocator fileQueueFactory, String filePath) {
        this.storeConfig = fileQueueFactory.getStoreConfig();
        this.flatFile = fileQueueFactory.createFlatFileForCommitLog(filePath);
        this.consumeQueueMinOffset = new AtomicLong(OFFSET_NOT_EXIST);
    }

    public TieredFlatFile getFlatFile() {
        return flatFile;
    }

    public long getMinOffset() {
        return flatFile.getMinOffset();
    }

    public long getCommitOffset() {
        return flatFile.getCommitOffset();
    }

    public long getMaxOffset() {
        return flatFile.getMaxOffset();
    }

    public long getMinConsumeQueueOffset() {
        return consumeQueueMinOffset.get() != OFFSET_NOT_EXIST ? consumeQueueMinOffset.get() : correctMinOffset();
    }

    public long correctMinOffset() {
        try {
            return correctMinOffsetAsync().get();
        } catch (Exception e) {
            log.error("Correct min offset failed in clean expired file", e);
        }
        return OFFSET_NOT_EXIST;
    }

    public long getMinTimestamp() {
        return flatFile.getMinTimestamp();
    }

    public long getMaxTimestamp() {
        return flatFile.getMaxTimestamp();
    }

    public synchronized CompletableFuture<Long> correctMinOffsetAsync() {
        if (flatFile.getFileSegmentCount() == 0) {
            this.consumeQueueMinOffset.set(OFFSET_NOT_EXIST);
            return CompletableFuture.completedFuture(OFFSET_NOT_EXIST);
        }

        // queue offset field length is 8
        int length = MessageFormatUtil.QUEUE_OFFSET_POSITION + 8;
        if (flatFile.getCommitOffset() - flatFile.getMinOffset() < length) {
            this.consumeQueueMinOffset.set(OFFSET_NOT_EXIST);
            return CompletableFuture.completedFuture(OFFSET_NOT_EXIST);
        }

        try {
            return this.flatFile.readAsync(this.flatFile.getMinOffset(), length)
                    .thenApply(buffer -> {
                        long offset = MessageFormatUtil.getQueueOffset(buffer);
                        consumeQueueMinOffset.set(offset);
                        log.debug("Correct commitlog min cq offset success, " +
                                        "filePath={}, min cq offset={}, commitlog range={}-{}",
                                flatFile.getFilePath(), offset, flatFile.getMinOffset(), flatFile.getCommitOffset());
                        return offset;
                    })
                    .exceptionally(throwable -> {
                        log.warn("Correct commitlog min cq offset error, filePath={}, range={}-{}",
                                flatFile.getFilePath(), flatFile.getMinOffset(), flatFile.getCommitOffset(), throwable);
                        return consumeQueueMinOffset.get();
                    });
        } catch (Exception e) {
            log.error("Correct commitlog min cq offset error, filePath={}", flatFile.getFilePath(), e);
        }
        return CompletableFuture.completedFuture(consumeQueueMinOffset.get());
    }

    public AppendResult append(ByteBuffer byteBuf) {
        return flatFile.append(byteBuf, MessageFormatUtil.getStoreTimeStamp(byteBuf));
    }

    public void commit(boolean sync) {
        flatFile.commit(sync);
    }

    public CompletableFuture<ByteBuffer> readAsync(long offset, int length) {
        return flatFile.readAsync(offset, length);
    }

    public void rollingNewFile() {
        TieredFileSegment fileSegment = flatFile.getFileToWrite();
        if (System.currentTimeMillis() - fileSegment.getMaxTimestamp() >
                TimeUnit.HOURS.toMillis(storeConfig.getCommitLogRollingInterval())
                && fileSegment.getAppendPosition() > storeConfig.getCommitLogRollingMinimumSize()) {
            flatFile.rollingNewFile();
        }
    }

    public void cleanExpiredFile(long expireTimestamp) {
        if (flatFile.cleanExpiredFile(expireTimestamp) > 0) {
            correctMinOffset();
        }
    }

    public void destroyExpiredFile() {
        flatFile.destroyExpiredFile();
    }

    public void destroy() {
        flatFile.destroy();
    }
}

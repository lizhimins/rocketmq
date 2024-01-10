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
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.provider.FileSegment;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtil;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;

import static org.apache.rocketmq.tieredstore.file.FlatMessageFile.OFFSET_NOT_EXIST;

public class FlatCommitLogFile {

    private static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    private final MessageStoreConfig storeConfig;
    private final FlatAppendFile flatAppendFile;
    private final AtomicLong consumeQueueMinOffset;

    public FlatCommitLogFile(FlatFileFactory fileQueueFactory, String filePath) {
        this.storeConfig = fileQueueFactory.getStoreConfig();
        this.flatAppendFile = fileQueueFactory.createFlatFileForCommitLog(filePath);
        this.consumeQueueMinOffset = new AtomicLong(OFFSET_NOT_EXIST);
    }

    public FlatAppendFile getFlatFile() {
        return flatAppendFile;
    }

    public long getMinOffset() {
        return flatAppendFile.getMinOffset();
    }

    public long getCommitOffset() {
        return flatAppendFile.getCommitOffset();
    }

    public long getMaxOffset() {
        return flatAppendFile.getMaxOffset();
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
        return flatAppendFile.getMinTimestamp();
    }

    public long getMaxTimestamp() {
        return flatAppendFile.getMaxTimestamp();
    }

    public synchronized CompletableFuture<Long> correctMinOffsetAsync() {
//        if (flatCompositeFile.getFileSegmentCount() == 0) {
//            this.consumeQueueMinOffset.set(OFFSET_NOT_EXIST);
//            return CompletableFuture.completedFuture(OFFSET_NOT_EXIST);
//        }

        // queue offset field length is 8
        int length = MessageFormatUtil.QUEUE_OFFSET_POSITION + 8;
        if (flatAppendFile.getCommitOffset() - flatAppendFile.getMinOffset() < length) {
            this.consumeQueueMinOffset.set(OFFSET_NOT_EXIST);
            return CompletableFuture.completedFuture(OFFSET_NOT_EXIST);
        }

        try {
            return this.flatAppendFile.readAsync(this.flatAppendFile.getMinOffset(), length)
                    .thenApply(buffer -> {
                        long offset = MessageFormatUtil.getQueueOffset(buffer);
                        consumeQueueMinOffset.set(offset);
                        log.debug("Correct commitlog min cq offset success, " +
                                        "filePath={}, min cq offset={}, commitlog range={}-{}",
                                flatAppendFile.getFilePath(), offset, flatAppendFile.getMinOffset(), flatAppendFile.getCommitOffset());
                        return offset;
                    })
                    .exceptionally(throwable -> {
                        log.warn("Correct commitlog min cq offset error, filePath={}, range={}-{}",
                                flatAppendFile.getFilePath(), flatAppendFile.getMinOffset(), flatAppendFile.getCommitOffset(), throwable);
                        return consumeQueueMinOffset.get();
                    });
        } catch (Exception e) {
            log.error("Correct commitlog min cq offset error, filePath={}", flatAppendFile.getFilePath(), e);
        }
        return CompletableFuture.completedFuture(consumeQueueMinOffset.get());
    }

    public AppendResult append(ByteBuffer byteBuf) {
        return flatAppendFile.append(byteBuf, MessageFormatUtil.getStoreTimeStamp(byteBuf));
    }

    public void commit(boolean sync) {
        flatAppendFile.commit(sync);
    }

    public CompletableFuture<ByteBuffer> readAsync(long offset, int length) {
        return flatAppendFile.readAsync(offset, length);
    }

    public void rollingNewFile() {
        FileSegment fileSegment = flatAppendFile.getFileToWrite();
        if (System.currentTimeMillis() - fileSegment.getMaxTimestamp() >
                TimeUnit.HOURS.toMillis(storeConfig.getCommitLogRollingInterval())
                && fileSegment.getAppendPosition() > storeConfig.getCommitLogRollingMinimumSize()) {
            flatAppendFile.rollingNewFile();
        }
    }

    public void cleanExpiredFile(long expireTimestamp) {
        if (flatAppendFile.cleanExpiredFile(expireTimestamp) > 0) {
            correctMinOffset();
        }
    }

    public void destroyExpiredFile() {
        flatAppendFile.destroyExpiredFile();
    }

    public void destroy() {
        flatAppendFile.destroy();
    }
}

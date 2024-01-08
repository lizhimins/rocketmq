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
package org.apache.rocketmq.tieredstore.provider;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.exception.MessageStoreErrorCode;
import org.apache.rocketmq.tieredstore.exception.MessageStoreException;
import org.apache.rocketmq.tieredstore.file.FlatConsumeQueueFile;
import org.apache.rocketmq.tieredstore.stream.FileSegmentInputStream;
import org.apache.rocketmq.tieredstore.stream.FileSegmentInputStreamFactory;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.tieredstore.index.IndexStoreFile.INDEX_BEGIN_TIME_STAMP;
import static org.apache.rocketmq.tieredstore.index.IndexStoreFile.INDEX_END_TIME_STAMP;

public abstract class FileSegment implements Comparable<FileSegment>, FileSegmentProvider {

    private static final Logger log = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    protected final String filePath;
    protected final long baseOffset;
    protected final FileSegmentType fileType;
    protected final MessageStoreConfig storeConfig;

    private final long maxSize;
    private final ReentrantLock fileLock = new ReentrantLock();
    private final Semaphore commitLock = new Semaphore(1);

    private volatile boolean closed = true;
    private volatile boolean deleted = false;

    private volatile long minTimestamp = Long.MAX_VALUE;
    private volatile long maxTimestamp = Long.MAX_VALUE;
    private volatile long commitPosition = 0L;
    private volatile long appendPosition = 0L;

    private volatile List<ByteBuffer> bufferList = new ArrayList<>();
    private volatile FileSegmentInputStream fileSegmentInputStream;
    private volatile CompletableFuture<Boolean> flightCommitRequest = CompletableFuture.completedFuture(false);

    public FileSegment(MessageStoreConfig storeConfig, FileSegmentType fileType, String filePath, long baseOffset) {
        this.storeConfig = storeConfig;
        this.fileType = fileType;
        this.filePath = filePath;
        this.baseOffset = baseOffset;
        this.maxSize = FileSegmentProvider.getMaxSizeByFileType(storeConfig, fileType);
    }

    @Override
    public int compareTo(FileSegment o) {
        return Long.compare(this.baseOffset, o.baseOffset);
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public long getCommitOffset() {
        return baseOffset + commitPosition;
    }

    public long getCommitPosition() {
        return commitPosition;
    }

    public long getMaxOffset() {
        return baseOffset + appendPosition;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public void setMinTimestamp(long minTimestamp) {
        this.minTimestamp = minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public void setMaxTimestamp(long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }

    public boolean isClosed() {
        return closed;
    }

    public void setClosed(boolean closed) {
        this.closed = closed;
    }

    //public void markSealed() {
    //    this.markSealed(true);
    //}
    //
    //public void markSealed(boolean appendCoda) {
    //    segmentLock.lock();
    //    try {
    //        closed = true;
    //        if (fileType == FileSegmentType.COMMIT_LOG && appendCoda) {
    //            appendCoda();
    //        }
    //    } finally {
    //        segmentLock.unlock();
    //    }
    //}

    public boolean isDeleted() {
        return deleted;
    }

    public void markDeleted(boolean deleted) {
        fileLock.lock();
        try {
            this.deleted = deleted;
        } finally {
            fileLock.unlock();
        }
    }

    public FileSegmentType getFileType() {
        return fileType;
    }

    public void initPosition(long pos) {
        this.commitPosition = pos;
        this.appendPosition = pos;
    }

    private List<ByteBuffer> borrowBuffer() {
        fileLock.lock();
        try {
            List<ByteBuffer> tmp = bufferList;
            bufferList = new ArrayList<>();
            return tmp;
        } finally {
            fileLock.unlock();
        }
    }

    public AppendResult append(ByteBuffer buffer, long timestamp) {
        fileLock.lock();
        try {
            if (closed) {
                return AppendResult.FILE_CLOSED;
            }

            // IndexFile is large and not change after compaction, no need deep copy
            if (fileType == FileSegmentType.INDEX) {
                minTimestamp = buffer.getLong(INDEX_BEGIN_TIME_STAMP);
                maxTimestamp = buffer.getLong(INDEX_END_TIME_STAMP);
                appendPosition += buffer.remaining();
                bufferList.add(buffer);
                return AppendResult.SUCCESS;
            }

            if (appendPosition + buffer.remaining() > maxSize) {
                return AppendResult.FILE_FULL;
            }

            // The number of messages exceeds the low watermark, or the size of cached buffers is too large.
            if (bufferList.size() > storeConfig.getTieredStoreGroupCommitCount() ||
                appendPosition - commitPosition > storeConfig.getTieredStoreGroupCommitSize()) {
                commitAsync();
            }

            if (bufferList.size() > storeConfig.getTieredStoreMaxGroupCommitCount()) {
                return AppendResult.BUFFER_FULL;
            }

            if (timestamp != Long.MAX_VALUE) {
                maxTimestamp = timestamp;
                if (minTimestamp == Long.MAX_VALUE) {
                    minTimestamp = timestamp;
                }
            }

            appendPosition += buffer.remaining();

            // deep copy buffer
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(buffer.remaining());
            byteBuffer.put(buffer);
            byteBuffer.flip();
            buffer.rewind();

            bufferList.add(byteBuffer);
            return AppendResult.SUCCESS;
        } finally {
            fileLock.unlock();
        }
    }

    public void setCommitPosition(long commitPosition) {
        this.commitPosition = commitPosition;
    }

    public long getAppendPosition() {
        return appendPosition;
    }

    public void setAppendPosition(long appendPosition) {
        this.appendPosition = appendPosition;
    }

    public ByteBuffer read(long position, int length) {
        return readAsync(position, length).join();
    }

    public CompletableFuture<ByteBuffer> readAsync(long position, int length) {
        CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
        if (position < 0 || length < 0) {
            future.completeExceptionally(
                new MessageStoreException(MessageStoreErrorCode.ILLEGAL_PARAM, "position or length is negative"));
            return future;
        }
        if (length == 0) {
            future.completeExceptionally(
                new MessageStoreException(MessageStoreErrorCode.ILLEGAL_PARAM, "length is zero"));
            return future;
        }
        if (position >= commitPosition) {
            future.completeExceptionally(
                new MessageStoreException(MessageStoreErrorCode.ILLEGAL_PARAM, "position is illegal"));
            return future;
        }
        if (position + length > commitPosition) {
            log.debug("TieredFileSegment#readAsync request position + length is greater than commit position," +
                    " correct length using commit position, file: {}, request position: {}, commit position:{}, change length from {} to {}",
                getPath(), position, commitPosition, length, commitPosition - position);
            length = (int) (commitPosition - position);
            if (length == 0) {
                future.completeExceptionally(
                    new MessageStoreException(MessageStoreErrorCode.NO_NEW_DATA, "request position is equal to commit position"));
                return future;
            }
            if (fileType == FileSegmentType.CONSUME_QUEUE && length % FlatConsumeQueueFile.CONSUME_QUEUE_STORE_UNIT_SIZE != 0) {
                future.completeExceptionally(
                    new MessageStoreException(MessageStoreErrorCode.ILLEGAL_PARAM, "position and length is illegal"));
                return future;
            }
        }
        return read0(position, length);
    }

    public boolean needCommit() {
        return appendPosition > commitPosition;
    }

    public boolean commit() {
        if (closed) {
            return false;
        }
        // result is false when we send real commit request
        // use join for wait flight request done
        Boolean result = commitAsync().join();
        if (!result) {
            result = flightCommitRequest.join();
        }
        return result;
    }

    private void releaseCommitLock() {
        if (commitLock.availablePermits() == 0) {
            commitLock.release();
        } else {
            log.error("[Bug] FileSegmentCommitAsync, lock is already released: available permits: {}",
                commitLock.availablePermits());
        }
    }

    /**
     * @return false: commit, true: no commit operation
     */
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public CompletableFuture<Boolean> commitAsync() {
        if (closed) {
            return CompletableFuture.completedFuture(false);
        }

        if (!needCommit()) {
            return CompletableFuture.completedFuture(true);
        }

        if (commitLock.drainPermits() <= 0) {
            return CompletableFuture.completedFuture(false);
        }

        try {
            if (fileSegmentInputStream != null) {
                long fileSize = this.getSize();
                if (fileSize == -1L) {
                    log.error("Get commit position error before commit, Commit: %d, Expect: %d, Current Max: %d, FileName: %s",
                        commitPosition, commitPosition + fileSegmentInputStream.getContentLength(), appendPosition, getPath());
                    releaseCommitLock();
                    return CompletableFuture.completedFuture(false);
                } else {
                    if (correctPosition(fileSize, null)) {
                        fileSegmentInputStream = null;
                    }
                }
            }

            int bufferSize;
            if (fileSegmentInputStream != null) {
                bufferSize = fileSegmentInputStream.available();
            } else {
                List<ByteBuffer> bufferList = borrowBuffer();
                bufferSize = bufferList.stream().mapToInt(ByteBuffer::remaining).sum();
                if (bufferSize == 0) {
                    releaseCommitLock();
                    return CompletableFuture.completedFuture(true);
                }
                fileSegmentInputStream = FileSegmentInputStreamFactory.build(
                    fileType, baseOffset + commitPosition, bufferList, null, bufferSize);
            }

            return flightCommitRequest = this
                .commit0(fileSegmentInputStream, commitPosition, bufferSize, fileType != FileSegmentType.INDEX)
                .thenApply(result -> {
                    if (result) {
                        commitPosition += bufferSize;
                        fileSegmentInputStream = null;
                        return true;
                    } else {
                        fileSegmentInputStream.rewind();
                        return false;
                    }
                })
                .exceptionally(this::handleCommitException)
                .whenComplete((result, e) -> releaseCommitLock());

        } catch (Exception e) {
            handleCommitException(e);
            releaseCommitLock();
        }
        return CompletableFuture.completedFuture(false);
    }

    private long getCorrectFileSize(Throwable throwable) {
        if (throwable instanceof MessageStoreException) {
            long fileSize = ((MessageStoreException) throwable).getPosition();
            if (fileSize > 0) {
                return fileSize;
            }
        }
        return getSize();
    }

    private boolean handleCommitException(Throwable e) {
        // Get root cause here
        Throwable cause = e.getCause() != null ? e.getCause() : e;
        long fileSize = this.getCorrectFileSize(cause);

        if (fileSize == -1L) {
            log.error("Get commit position error, Commit: %d, Expect: %d, Current Max: %d, FileName: %s",
                commitPosition, commitPosition + fileSegmentInputStream.getContentLength(), appendPosition, getPath());
            fileSegmentInputStream.rewind();
            return false;
        }

        if (correctPosition(fileSize, cause)) {
            // updateDispatchCommitOffset(fileSegmentInputStream.getBufferList());
            fileSegmentInputStream = null;
            return true;
        } else {
            fileSegmentInputStream.rewind();
            return false;
        }
    }

    /**
     * return true to clear buffer
     */
    private boolean correctPosition(long fileSize, Throwable throwable) {

        // Current we have three offsets here: commit offset, expect offset, file size.
        // We guarantee that the commit offset is less than or equal to the expect offset.
        // Max offset will increase because we can continuously put in new buffers
        String handleInfo = throwable == null ? "before commit" : "after commit";
        long expectPosition = commitPosition + fileSegmentInputStream.getContentLength();

        String offsetInfo = String.format("Correct Commit Position, %s, result=[{}], " +
                "Commit: %d, Expect: %d, Current Max: %d, FileSize: %d, FileName: %s",
            handleInfo, commitPosition, expectPosition, appendPosition, fileSize, this.getPath());

        // We are believing that the file size returned by the server is correct,
        // can reset the commit offset to the file size reported by the storage system.
        if (fileSize == expectPosition) {
            log.info(offsetInfo, "Success", throwable);
            commitPosition = fileSize;
            return true;
        }

        if (fileSize < commitPosition) {
            log.error(offsetInfo, "FileSizeIncorrect", throwable);
        } else if (fileSize == commitPosition) {
            log.warn(offsetInfo, "CommitFailed", throwable);
        } else if (fileSize > commitPosition) {
            log.warn(offsetInfo, "PartialSuccess", throwable);
        }
        commitPosition = fileSize;
        return false;
    }
}

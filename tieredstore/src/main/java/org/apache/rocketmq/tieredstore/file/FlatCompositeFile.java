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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.metadata.entity.FileSegmentMetadata;
import org.apache.rocketmq.tieredstore.provider.FileSegment;
import org.apache.rocketmq.tieredstore.provider.FileSegmentFactory;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;

public class FlatCompositeFile {

    private static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);
    private static final long OFFSET_NOT_EXIST = -1L;

    private final String filePath;
    private final FileSegmentType fileType;
    private final MetadataStore metadataStore;
    private final FileSegmentFactory fileSegmentFactory;
    private final ReentrantReadWriteLock fileSegmentLock;
    private final ConcurrentNavigableMap<Long, FileSegment> fileSegmentTable;

    public FlatCompositeFile(FileSegmentFactory fileSegmentFactory, FileSegmentType fileType, String filePath) {
        this.fileType = fileType;
        this.filePath = filePath;
        this.metadataStore = fileSegmentFactory.getMetadataStore();
        this.fileSegmentFactory = fileSegmentFactory;
        this.fileSegmentLock = new ReentrantReadWriteLock();
        this.fileSegmentTable = new ConcurrentSkipListMap<>();
        this.recoverMetadata();
    }

    public String getFilePath() {
        return filePath;
    }

    public FileSegmentType getFileType() {
        return fileType;
    }

    public long getMinOffset() {
        fileSegmentLock.readLock().lock();
        try {
            if (fileSegmentTable.isEmpty()) {
                return OFFSET_NOT_EXIST;
            }
            return fileSegmentTable.firstKey();
        } finally {
            fileSegmentLock.readLock().unlock();
        }
    }

    public long getCommitOffset() {
        fileSegmentLock.readLock().lock();
        try {
            if (fileSegmentTable.isEmpty()) {
                return OFFSET_NOT_EXIST;
            }
            return fileSegmentTable.lastEntry().getValue().getCommitOffset();
        } finally {
            fileSegmentLock.readLock().unlock();
        }
    }

    public long getMaxOffset() {
        fileSegmentLock.readLock().lock();
        try {
            if (fileSegmentTable.isEmpty()) {
                return OFFSET_NOT_EXIST;
            }
            return fileSegmentTable.lastEntry().getValue().getAppendOffset();
        } finally {
            fileSegmentLock.readLock().unlock();
        }
    }

    protected void recoverMetadata() {
        metadataStore.iterateFileSegment(filePath, fileType, metadata -> {
            FileSegment segment =
                this.newSegment(fileType, metadata.getBaseOffset(), false);
            segment.initPosition(metadata.getSize());
            segment.setMinTimestamp(metadata.getBeginTimestamp());
            segment.setMaxTimestamp(metadata.getEndTimestamp());
            fileSegmentTable.put(metadata.getBaseOffset(), segment);
        });

        if (fileType != FileSegmentType.INDEX) {
            correctFileSize();
        }
    }

    /**
     * FileQueue Status: Sealed | Sealed | Sealed | Not sealed, Allow appended && Not Full
     */
    public void updateFileSegment(FileSegment fileSegment) {
        FileSegmentMetadata metadata = metadataStore.getFileSegment(
            this.filePath, fileSegment.getFileType(), fileSegment.getBaseOffset());
        if (metadata == null) {
            metadata = new FileSegmentMetadata(
                this.filePath, fileSegment.getBaseOffset(), fileSegment.getFileType().getCode());
            metadata.setCreateTimestamp(System.currentTimeMillis());
        }
        metadata.setSize(fileSegment.getCommitPosition());
        metadata.setBeginTimestamp(fileSegment.getMinTimestamp());
        metadata.setEndTimestamp(fileSegment.getMaxTimestamp());
        this.metadataStore.updateFileSegment(metadata);
    }

    private void correctFileSize() {
        //for (int i = 1; i < fileSegmentList.size(); i++) {
        //    TieredFileSegment pre = fileSegmentList.get(i - 1);
        //    TieredFileSegment cur = fileSegmentList.get(i);
        //
        //    if (pre.getCommitOffset() != cur.getBaseOffset()) {
        //        try {
        //            long actualSize = pre.getSize();
        //            if (pre.getBaseOffset() + actualSize == cur.getBaseOffset()) {
        //                pre.initPosition(actualSize);
        //                this.updateFileSegment(pre);
        //                log.info("TieredFlatFile#correctFileSize, correct file size when construct file, " +
        //                                "filePath: {}, file type: {}, base offset: {}, actual size: {}, next file offset: {}",
        //                        filePath, fileType, pre.getBaseOffset(), actualSize, cur.getBaseOffset());
        //            } else {
        //                log.error("TieredFlatFile#correctFileSize: " +
        //                                "file segment has incorrect size and can not fix: " +
        //                                "filePath:{}, file type: {}, base offset: {}, actual size: {}, next file offset: {}",
        //                        filePath, fileType, pre.getBaseOffset(), actualSize, cur.getBaseOffset());
        //            }
        //        } catch (Exception e) {
        //            log.error("TieredFlatFile#correctFileSize: " +
        //                            "fix file segment size failed: filePath: {}, file type: {}, base offset: {}",
        //                    filePath, fileType, pre.getBaseOffset());
        //        }
        //    }
        //}
        //
        //// correct last
        //if (!fileSegmentList.isEmpty()) {
        //    TieredFileSegment fileSegment = fileSegmentList.get(fileSegmentList.size() - 1);
        //    long fileSize = fileSegment.getSize();
        //    if (fileSize != -1L && fileSize != fileSegment.getCommitPosition()) {
        //        fileSegment.initPosition(fileSize);
        //        this.updateFileSegment(fileSegment);
        //        log.warn("Correct file size");
        //    }
        //}
    }

    private FileSegment newSegment(FileSegmentType fileType, long baseOffset, boolean createMetadata) {
        FileSegment segment =
            fileSegmentFactory.createSegment(fileType, filePath, baseOffset);
        if (fileType != FileSegmentType.INDEX) {
            segment.createFile();
        }
        if (createMetadata) {
            this.updateFileSegment(segment);
        }
        return segment;
    }

    public void rollingNewFile() {
        fileSegmentLock.writeLock().lock();
        try {
            // this.getFileToWrite().markSealed();
            this.getFileToWrite();
        } finally {
            fileSegmentLock.writeLock().unlock();
        }
    }

    protected long getMinTimestamp() {
        return 0;
    }

    protected long getMaxTimestamp() {
        return 0;
    }

    protected FileSegment getFileToWrite() {
//        if (baseOffset == -1) {
//            throw new IllegalStateException("need to set base offset before create file segment");
//        }

        FileSegment fileSegment;
        fileSegmentLock.readLock().lock();
        try {
//            if (!fileSegmentList.isEmpty()) {
//                fileSegment = fileSegmentList.get(fileSegmentList.size() - 1);
//                if (!fileSegment.isFull()) {
//                    return fileSegment;
//                }
//            }
        } finally {
            fileSegmentLock.readLock().unlock();
        }

        fileSegmentLock.writeLock().lock();
        try {
//            long offset = baseOffset;
//            if (!fileSegmentList.isEmpty()) {
//                fileSegment = fileSegmentList.get(fileSegmentList.size() - 1);
//                if (fileSegment.isFull()) {
//                    if (fileSegment.commit()) {
//                        this.updateFileSegment(fileSegment);
//                    }
//                } else {
//                    return fileSegment;
//                }
//                offset = fileSegment.getMaxOffset();
//            }
//            fileSegment = this.newSegment(fileType, offset, true);
//            fileSegmentList.add(fileSegment);
//            needCommitFileSegmentList.add(fileSegment);
        } finally {
            fileSegmentLock.writeLock().unlock();
        }
        return null;
    }

    @Nullable
    protected FileSegment getFileByTime(long timestamp, BoundaryType boundaryType) {
        fileSegmentLock.readLock().lock();
        try {
//            List<FileSegment> segmentList = fileSegmentList.stream()
//                .sorted(boundaryType == BoundaryType.UPPER ?
//                    Comparator.comparingLong(FileSegment::getMaxTimestamp) :
//                    Comparator.comparingLong(FileSegment::getMinTimestamp))
//                .filter(segment -> boundaryType == BoundaryType.UPPER ?
//                    segment.getMaxTimestamp() >= timestamp : segment.getMinTimestamp() <= timestamp)
//                .collect(Collectors.toList());
//
//            if (!segmentList.isEmpty()) {
//                return boundaryType == BoundaryType.UPPER ? segmentList.get(0) : segmentList.get(segmentList.size() - 1);
//            }
//
//            if (fileSegmentList.isEmpty()) {
//                return null;
//            }
//            return boundaryType == BoundaryType.UPPER ?
//                fileSegmentList.get(fileSegmentList.size() - 1) : fileSegmentList.get(0);
        } finally {
            fileSegmentLock.readLock().unlock();
        }

        return null;
    }

    public AppendResult append(ByteBuffer byteBuf) {
        return append(byteBuf, Long.MAX_VALUE, false);
    }

    public AppendResult append(ByteBuffer byteBuf, long timeStamp) {
        return append(byteBuf, timeStamp, false);
    }

    public AppendResult append(ByteBuffer byteBuf, long timeStamp, boolean commit) {
        FileSegment fileSegment = getFileToWrite();
        AppendResult result = fileSegment.append(byteBuf, timeStamp);
        if (commit && result == AppendResult.BUFFER_FULL && fileSegment.commit()) {
            result = fileSegment.append(byteBuf, timeStamp);
        }
        if (result == AppendResult.FILE_FULL) {
            // write to new file
            return getFileToWrite().append(byteBuf, timeStamp);
        }
        return result;
    }

    public int cleanExpiredFile(long expireTimestamp) {
        Set<Long> needToDeleteSet = new HashSet<>();
        try {
            metadataStore.iterateFileSegment(filePath, fileType, metadata -> {
                if (metadata.getEndTimestamp() < expireTimestamp) {
                    needToDeleteSet.add(metadata.getBaseOffset());
                }
            });
        } catch (Exception e) {
            log.error("Clean expired file, filePath: {}, file type: {}, expire timestamp: {}",
                filePath, fileType, expireTimestamp);
        }

        if (needToDeleteSet.isEmpty()) {
            return 0;
        }

        fileSegmentLock.writeLock().lock();
        try {
//            for (int i = 0; i < fileSegmentList.size(); i++) {
//                FileSegment fileSegment = fileSegmentList.get(i);
//                try {
//                    if (needToDeleteSet.contains(fileSegment.getBaseOffset())) {
//                        fileSegment.close();
//                        fileSegmentList.remove(fileSegment);
//                        needCommitFileSegmentList.remove(fileSegment);
//                        i--;
//                        this.updateFileSegment(fileSegment);
//                        log.debug("Clean expired file, filePath: {}", fileSegment.getPath());
//                    } else {
//                        break;
//                    }
//                } catch (Exception e) {
//                    log.error("Clean expired file failed: filePath: {}, file type: {}, expire timestamp: {}",
//                        fileSegment.getPath(), fileSegment.getFileType(), expireTimestamp, e);
//                }
//            }
//            if (!fileSegmentList.isEmpty()) {
//                baseOffset = fileSegmentList.get(0).getBaseOffset();
//            } else if (fileType == FileSegmentType.CONSUME_QUEUE) {
//                baseOffset = -1;
//            } else {
//                baseOffset = 0;
//            }
        } finally {
            fileSegmentLock.writeLock().unlock();
        }
        return needToDeleteSet.size();
    }

    public void destroyExpiredFile() {
        try {
            metadataStore.iterateFileSegment(filePath, fileType, metadata -> {
                if (metadata.getStatus() == FileSegmentMetadata.STATUS_DELETED) {
                    try {
                        FileSegment fileSegment =
                            this.newSegment(fileType, metadata.getBaseOffset(), false);
                        fileSegment.destroyFile();
                        if (!fileSegment.exists()) {
                            metadataStore.deleteFileSegment(filePath, fileType, metadata.getBaseOffset());
                        }
                    } catch (Exception e) {
                        log.error("Destroyed expired file failed, file path: {}, file type: {}",
                            filePath, fileType, e);
                    }
                }
            });
        } catch (Exception e) {
            log.error("Destroyed expired file, file path: {}, file type: {}", filePath, fileType);
        }
    }

    public void commit(boolean sync) {
        ArrayList<CompletableFuture<Void>> futureList = new ArrayList<>();
        try {
//            for (FileSegment segment : needCommitFileSegmentList) {
//                if (segment.isClosed()) {
//                    continue;
//                }
//                futureList.add(segment
//                    .commitAsync()
//                    .thenAccept(success -> {
//                        this.updateFileSegment(segment);
//                        if (segment.isFull() && !segment.needCommit()) {
//                            needCommitFileSegmentList.remove(segment);
//                        }
//                    })
//                );
//            }
        } catch (Exception e) {
            log.error("Commit file segment failed: topic: {}, queue: {}, file type: {}", filePath, fileType, e);
        }
        if (sync) {
            CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
        }
    }

    public CompletableFuture<ByteBuffer> readAsync(long offset, int length) {
//        int index = getSegmentIndexByOffset(offset);
//        if (index == -1) {
//            String errorMsg = String.format("TieredFlatFile#readAsync: offset is illegal, " +
//                            "file path: %s, file type: %s, start: %d, length: %d, file num: %d",
//                    filePath, fileType, offset, length, fileSegmentList.size());
//            log.error(errorMsg);
//            throw new TieredStoreException(TieredStoreErrorCode.ILLEGAL_OFFSET, errorMsg);
//        }
//        TieredFileSegment fileSegment1;
//        TieredFileSegment fileSegment2 = null;
//        fileSegmentLock.readLock().lock();
//        try {
//            fileSegment1 = fileSegmentList.get(index);
//            if (offset + length > fileSegment1.getCommitOffset()) {
//                if (fileSegmentList.size() > index + 1) {
//                    fileSegment2 = fileSegmentList.get(index + 1);
//                }
//            }
//        } finally {
//            fileSegmentLock.readLock().unlock();
//        }
//        if (fileSegment2 == null) {
//            return fileSegment1.readAsync(offset - fileSegment1.getBaseOffset(), length);
//        }
//        int segment1Length = (int) (fileSegment1.getCommitOffset() - offset);
//        return fileSegment1.readAsync(offset - fileSegment1.getBaseOffset(), segment1Length)
//                .thenCombine(fileSegment2.readAsync(0, length - segment1Length), (buffer1, buffer2) -> {
//                    ByteBuffer compositeBuffer = ByteBuffer.allocate(buffer1.remaining() + buffer2.remaining());
//                    compositeBuffer.put(buffer1).put(buffer2);
//                    compositeBuffer.flip();
//                    return compositeBuffer;
//                });
        return CompletableFuture.completedFuture(null);
    }

    public void destroy() {
        fileSegmentLock.writeLock().lock();
        try {
            while (!fileSegmentTable.isEmpty()) {
                FileSegment fileSegment = fileSegmentTable.firstEntry().getValue();
                try {
                    fileSegment.markDeleted();
                    fileSegment.destroyFile();
                    if (!fileSegment.exists()) {
                        fileSegmentTable.remove(fileSegment.getBaseOffset());
                        metadataStore.deleteFileSegment(filePath, fileType, fileSegment.getBaseOffset());
                    }
                } catch (Exception e) {
                    log.error("Destroy segment file error, filePath: {}", fileSegment.getPath(), e);
                }
            }
        } finally {
            fileSegmentLock.writeLock().unlock();
        }
    }
}

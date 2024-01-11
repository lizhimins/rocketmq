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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
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

public class FlatAppendFile {

    protected static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);
    public static final long OFFSET_NOT_EXIST = -1L;

    private final String filePath;
    private final FileSegmentType fileType;
    private final MetadataStore metadataStore;
    private final FileSegmentFactory fileSegmentFactory;
    private final ReentrantReadWriteLock fileSegmentLock;
    private final CopyOnWriteArrayList<FileSegment> fileSegmentTable;

    public FlatAppendFile(FileSegmentFactory fileSegmentFactory, FileSegmentType fileType, String filePath) {
        this.fileType = fileType;
        this.filePath = filePath;
        this.metadataStore = fileSegmentFactory.getMetadataStore();
        this.fileSegmentFactory = fileSegmentFactory;
        this.fileSegmentLock = new ReentrantReadWriteLock();
        this.fileSegmentTable = new CopyOnWriteArrayList<>();
        this.recover();
    }

    public void recover() {
        List<FileSegment> fileSegmentList = new ArrayList<>();
        this.metadataStore.iterateFileSegment(this.filePath, this.fileType, metadata -> {
            FileSegment fileSegment = this.fileSegmentFactory.createSegment(
                this.fileType, metadata.getPath(), metadata.getBaseOffset());
            fileSegment.initPosition(metadata.getSize());
            fileSegment.setMinTimestamp(metadata.getBeginTimestamp());
            fileSegment.setMaxTimestamp(metadata.getEndTimestamp());
            fileSegmentList.add(fileSegment);
        });
        this.fileSegmentTable.addAll(fileSegmentList.stream().sorted().collect(Collectors.toList()));
        this.correctFileSegmentSize();
    }

    public void correctFileSegmentSize() {
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

    public void flushFileSegmentMeta(FileSegment fileSegment) {
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

    public String getFilePath() {
        return filePath;
    }

    public FileSegmentType getFileType() {
        return fileType;
    }

    public long getMinOffset() {
        List<FileSegment> list = this.fileSegmentTable;
        return list.isEmpty() ? OFFSET_NOT_EXIST : list.get(0).getBaseOffset();
    }

    public long getCommitOffset() {
        List<FileSegment> list = this.fileSegmentTable;
        return list.isEmpty() ? OFFSET_NOT_EXIST : list.get(list.size() - 1).getCommitOffset();
    }

    public long getAppendOffset() {
        List<FileSegment> list = this.fileSegmentTable;
        return list.isEmpty() ? OFFSET_NOT_EXIST : list.get(list.size() - 1).getAppendOffset();
    }

    public long getMinTimestamp() {
        List<FileSegment> list = this.fileSegmentTable;
        return list.isEmpty() ? OFFSET_NOT_EXIST : list.get(0).getMinTimestamp();
    }

    public long getMaxTimestamp() {
        List<FileSegment> list = this.fileSegmentTable;
        return list.isEmpty() ? OFFSET_NOT_EXIST : list.get(list.size() - 1).getMaxTimestamp();
    }

    public void rollingNewFile() {
        fileSegmentLock.writeLock().lock();
        try {
            // todo:
            // this.getFileToWrite().commit();
            this.getFileToWrite();
        } finally {
            fileSegmentLock.writeLock().unlock();
        }
    }

    public FileSegment getFileToWrite() {
        List<FileSegment> fileSegmentList = this.fileSegmentTable;
        if (fileSegmentList.isEmpty()) {
            throw new IllegalStateException("Need to set base offset before create file segment");
        } else {
            return fileSegmentList.get(fileSegmentList.size() - 1);
        }
    }

    public FileSegment getFileByTimestamp(long timestamp, BoundaryType boundaryType) {
        // todo: ???
        return null;
    }

    public AppendResult append(ByteBuffer buffer, long timestamp) {
        FileSegment fileSegment = this.getFileToWrite();
        AppendResult result = fileSegment.append(buffer, timestamp);
        if (result == AppendResult.FILE_FULL) {
            this.rollingNewFile();
            return getFileToWrite().append(buffer, timestamp);
        }
        return result;
    }

    public void commit(boolean sync) {
//        ArrayList<CompletableFuture<Void>> futureList = new ArrayList<>();
//        try {
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
//        } catch (Exception e) {
//            log.error("Commit file segment failed: topic: {}, queue: {}, file type: {}", filePath, fileType, e);
//        }
//        if (sync) {
//            CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
//        }
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

    public void destroyExpiredFile(long expireTimestamp) {
        // first remove expired file from fileSegmentTable
        // then close and delete expired file
//        Set<Long> needToDeleteSet = new HashSet<>();
//        try {
//            metadataStore.iterateFileSegment(filePath, fileType, metadata -> {
//                if (metadata.getEndTimestamp() < expireTimestamp) {
//                    needToDeleteSet.add(metadata.getBaseOffset());
//                }
//            });
//        } catch (Exception e) {
//            log.error("Clean expired file, filePath: {}, file type: {}, expire timestamp: {}",
//                filePath, fileType, expireTimestamp);
//        }
//
//        if (needToDeleteSet.isEmpty()) {
//            return 0;
//        }
//        fileSegmentLock.writeLock().lock();
//        try {
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
//        } finally {
//            fileSegmentLock.writeLock().unlock();
//        }
//        return needToDeleteSet.size();
//
//        try {
//            metadataStore.iterateFileSegment(filePath, fileType, metadata -> {
//                if (metadata.getStatus() == FileSegmentMetadata.STATUS_DELETED) {
//                    try {
//                        FileSegment fileSegment =
//                            this.newSegment(fileType, metadata.getBaseOffset(), false);
//                        fileSegment.destroyFile();
//                        if (!fileSegment.exists()) {
//                            metadataStore.deleteFileSegment(filePath, fileType, metadata.getBaseOffset());
//                        }
//                    } catch (Exception e) {
//                        log.error("Destroyed expired file failed, file path: {}, file type: {}",
//                            filePath, fileType, e);
//                    }
//                }
//            });
//        } catch (Exception e) {
//            log.error("Destroyed expired file, file path: {}, file type: {}", filePath, fileType);
//        }
    }

    public void destroy() {
//        fileSegmentLock.writeLock().lock();
//        try {
//            while (!fileSegmentTable.isEmpty()) {
//                FileSegment fileSegment = fileSegmentTable.firstEntry().getValue();
//                try {
//                    fileSegment.markDeleted();
//                    fileSegment.destroyFile();
//                    if (!fileSegment.exists()) {
//                        fileSegmentTable.remove(fileSegment.getBaseOffset());
//                        metadataStore.deleteFileSegment(filePath, fileType, fileSegment.getBaseOffset());
//                    }
//                } catch (Exception e) {
//                    log.error("Destroy segment file error, filePath: {}", fileSegment.getPath(), e);
//                }
//            }
//        } finally {
//            fileSegmentLock.writeLock().unlock();
//        }
    }
}

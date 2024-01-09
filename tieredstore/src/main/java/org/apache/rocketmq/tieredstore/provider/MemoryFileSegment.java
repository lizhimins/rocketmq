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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.stream.FileSegmentInputStream;
import org.apache.rocketmq.tieredstore.util.MessageStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryFileSegment extends FileSegment {

    private static final Logger log = LoggerFactory.getLogger(MessageStoreUtil.TIERED_STORE_LOGGER_NAME);

    protected final ByteBuffer memStore;
    protected CompletableFuture<Boolean> blocker;
    protected int size = 0;
    protected boolean checkSize = true;

    public MemoryFileSegment(MessageStoreConfig storeConfig,
        FileSegmentType fileType, String filePath, long baseOffset) {

        super(storeConfig, fileType, filePath, baseOffset);
        memStore = ByteBuffer.allocate(10000);
        memStore.position((int) getSize());
    }

    public ByteBuffer getMemStore() {
        return memStore;
    }

    public boolean isCheckSize() {
        return checkSize;
    }

    public void setCheckSize(boolean checkSize) {
        this.checkSize = checkSize;
    }

    public CompletableFuture<Boolean> getBlocker() {
        return blocker;
    }

    public void setBlocker(CompletableFuture<Boolean> blocker) {
        this.blocker = blocker;
    }

    @Override
    public String getPath() {
        return filePath;
    }

    @Override
    public long getSize() {
        if (checkSize) {
            return 1000;
        }
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public void createFile() {

    }

    @Override
    public CompletableFuture<ByteBuffer> read0(long position, int length) {
        ByteBuffer buffer = memStore.duplicate();
        buffer.position((int) position);
        ByteBuffer slice = buffer.slice();
        slice.limit(length);
        return CompletableFuture.completedFuture(slice);
    }

    @Override
    public CompletableFuture<Boolean> commit0(
        FileSegmentInputStream inputStream, long position, int length, boolean append) {

        try {
            if (blocker != null && !blocker.get()) {
                log.info("Commit Blocker Exception for Memory Test");
                return CompletableFuture.completedFuture(false);
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Commit Exception for Memory Test", e);
        }

        if (checkSize && position >= getSize()) {
            log.info("Commit Position Exception for Memory Test");
            return CompletableFuture.completedFuture(false);
        }

        byte[] buffer = new byte[1024];
        int startPos = memStore.position();
        try {
            int len;
            while ((len = inputStream.read(buffer)) > 0) {
                memStore.put(buffer, 0, len);
            }
            if (length != memStore.position() - startPos) {
                throw new CompletionException(new IllegalStateException());
            }
        } catch (Exception e) {
            return CompletableFuture.completedFuture(false);
        }
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public void destroyFile() {

    }

    //public MemoryFileSegmentWithoutCheck(FileSegmentType fileType,
    //                                     MessageQueue messageQueue, long baseOffset, MessageStoreConfig storeConfig) {
    //    super(storeConfig, fileType,
    //            storeConfig.getStorePathRootDir() + File.separator + TieredStoreUtil.toPath(messageQueue),
    //            baseOffset);
    //}
    //
    //public MemoryFileSegmentWithoutCheck(MessageStoreConfig storeConfig,
    //                                     FileSegmentType fileType, String filePath, long baseOffset) {
    //    super(storeConfig, fileType, filePath, baseOffset);
    //}

    //@Override
    //public long getSize() {
    //    return 0;
    //}
    //
    //@Override
    //public CompletableFuture<Boolean> commit0(FileSegmentInputStream inputStream, long position, int length,
    //                                          boolean append) {
    //    try {
    //        if (blocker != null && !blocker.get()) {
    //            throw new IllegalStateException();
    //        }
    //    } catch (InterruptedException | ExecutionException e) {
    //        Assert.fail(e.getMessage());
    //    }
    //
    //    byte[] buffer = new byte[1024];
    //
    //    int startPos = memStore.position();
    //    try {
    //        int len;
    //        while ((len = inputStream.read(buffer)) > 0) {
    //            memStore.put(buffer, 0, len);
    //        }
    //        Assert.assertEquals(length, memStore.position() - startPos);
    //    } catch (Exception e) {
    //        Assert.fail(e.getMessage());
    //        return CompletableFuture.completedFuture(false);
    //    }
    //    return CompletableFuture.completedFuture(true);
    //}
}

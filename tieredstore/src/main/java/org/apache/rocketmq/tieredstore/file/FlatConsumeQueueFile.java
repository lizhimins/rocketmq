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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.provider.FileSegment;

public class FlatConsumeQueueFile {

    /**
     * commit log offset: long, 8 bytes
     * message size: int, 4 bytes
     * tag hash code: long, 8 bytes
     */
    public static final int CONSUME_QUEUE_STORE_UNIT_SIZE = 8 + 4 + 8;

    private final FlatCompositeFile flatCompositeFile;

    public FlatConsumeQueueFile(FlatFileFactory fileQueueFactory, String filePath) {
        this.flatCompositeFile = fileQueueFactory.createFlatFileForConsumeQueue(filePath);
    }

    public boolean isInitialized() {
        return flatCompositeFile.getMinOffset() != -1L;
    }

    public FlatCompositeFile getFlatFile() {
        return flatCompositeFile;
    }

    public long getMinOffset() {
        return flatCompositeFile.getMinOffset();
    }

    public long getCommitOffset() {
        return flatCompositeFile.getCommitOffset();
    }

    public long getMaxOffset() {
        return flatCompositeFile.getMaxOffset();
    }

    public AppendResult append(final long offset, final int size, final long tagsCode, long timeStamp) {
        ByteBuffer cqItem = ByteBuffer.allocate(CONSUME_QUEUE_STORE_UNIT_SIZE);
        cqItem.putLong(offset).putInt(size).putLong(tagsCode).flip();
        return flatCompositeFile.append(cqItem, timeStamp, true);
    }

    public void commit(boolean sync) {
        flatCompositeFile.commit(sync);
    }

    public CompletableFuture<ByteBuffer> readAsync(long offset, int length) {
        return flatCompositeFile.readAsync(offset, length);
    }

    public void cleanExpiredFile(long expireTimestamp) {
        flatCompositeFile.cleanExpiredFile(expireTimestamp);
    }

    public void destroyExpiredFile() {
        flatCompositeFile.destroyExpiredFile();
    }

    protected Pair<Long, Long> getQueueOffsetInFileByTime(long timestamp, BoundaryType boundaryType) {
        FileSegment fileSegment = flatCompositeFile.getFileByTime(timestamp, boundaryType);
        if (fileSegment == null) {
            return Pair.of(-1L, -1L);
        }
        return Pair.of(fileSegment.getBaseOffset() / FlatConsumeQueueFile.CONSUME_QUEUE_STORE_UNIT_SIZE,
            fileSegment.getCommitOffset() / FlatConsumeQueueFile.CONSUME_QUEUE_STORE_UNIT_SIZE - 1);
    }

    public void destroy() {
        flatCompositeFile.destroy();
    }
}

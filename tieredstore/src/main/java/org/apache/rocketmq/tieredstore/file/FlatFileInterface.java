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
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.tieredstore.common.AppendResult;

public interface FlatFileInterface {

    long getConsumeQueueMinOffset();

    long getConsumeQueueMaxOffset();

    long getConsumeQueueCommitOffset();

    /**
     * Appends a message to the commit log file
     *
     * @param message the message to append
     * @return append result
     */
    AppendResult appendCommitLog(ByteBuffer message);

    /**
     * Append message to consume queue file, but does not commit it immediately
     *
     * @param request the dispatch request
     * @return append result
     */
    AppendResult appendConsumeQueue(DispatchRequest request);

    /**
     * Persist commit log file and consume queue file
     */
    CompletableFuture<Void> commitAsync();

    /**
     * Asynchronously retrieves the message at the specified consume queue offset
     *
     * @param consumeQueueOffset consume queue offset.
     * @return the message inner object serialized content
     */
    CompletableFuture<ByteBuffer> getMessageAsync(long consumeQueueOffset);

    /**
     * Get message from commitLog file at specified offset and length
     *
     * @param offset the offset
     * @param length the length
     * @return the message inner object serialized content
     */
    CompletableFuture<ByteBuffer> getCommitLogAsync(long offset, int length);

    /**
     * Asynchronously retrieves the consume queue message at the specified queue offset
     *
     * @param consumeQueueOffset consume queue offset.
     * @return the consumer queue unit serialized content
     */
    CompletableFuture<ByteBuffer> getConsumeQueueAsync(long consumeQueueOffset);

    /**
     * Asynchronously reads the message body from the consume queue file at the specified offset and count
     *
     * @param consumeQueueOffset the message offset
     * @param count              the number of messages to read
     * @return the consumer queue unit serialized content
     */
    CompletableFuture<ByteBuffer> getConsumeQueueAsync(long consumeQueueOffset, int count);

    /**
     * Gets the offset in the consume queue by timestamp and boundary type
     *
     * @param timestamp    search time
     * @param boundaryType lower or upper to decide boundary
     * @return Returns the offset of the message
     */
    CompletableFuture<Long> getOffsetInQueueByTime(long timestamp, BoundaryType boundaryType);

    /**
     * Shutdown process
     */
    void shutdown();

    /**
     * Destroys expired files
     */
    CompletableFuture<Void> destroyExpiredFile(long timestamp);

    /**
     * Delete file
     */
    CompletableFuture<Void> destroy();
}

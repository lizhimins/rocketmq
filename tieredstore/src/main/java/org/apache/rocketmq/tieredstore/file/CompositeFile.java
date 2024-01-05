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

public interface CompositeFile {

    /**
     * Initializes the offset for the flat file.
     * Will only affect the distribution site if the file has already been initialized.
     *
     * @param offset init offset for consume queue
     */
    void initOffset(long offset);

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
     * Return the consensus queue site corresponding to the confirmed site in the commitLog
     *
     * @return the maximum offset
     */
    long getDispatchOffset();

    /**
     * Return the consensus queue site corresponding to the confirmed site in the commitLog
     *
     * @return the maximum offset
     */
    long getDispatchCommitOffset();

    /**
     * Persist commit log file
     */
    void commitCommitLog();

    /**
     * Persist the consume queue file
     */
    void commitConsumeQueue();

    long getCommitLogMinOffset();

    long getCommitLogMaxOffset();

    long getCommitLogCommitOffset();

    long getConsumeQueueMinOffset();

    long getConsumeQueueMaxOffset();

    long getConsumeQueueCommitOffset();

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
    long getOffsetInConsumeQueueByTime(long timestamp, BoundaryType boundaryType);

    /**
     * Mark some commit log and consume file sealed and expired
     *
     * @param expireTimestamp expire timestamp, usually several days before the current time
     */
    void cleanExpiredFile(long expireTimestamp);

    /**
     * Destroys expired files
     */
    void destroyExpiredFile();

    /**
     * Shutdown process
     */
    void shutdown();

    /**
     * Delete file
     */
    void destroy();
}

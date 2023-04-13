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
package org.apache.rocketmq.tieredstore.container;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.BoundaryType;

interface FileChunk {

    void initOffset(long offset);

    /**
     * Append message to commitlog file, not commit immediately
     *
     * @param message the message to append
     * @return append result
     */
    AppendResult appendCommitLog(ByteBuffer message);

    /**
     * Append message to commitlog file and commit sync
     *
     * @param message the message to append
     * @return append result
     */
    AppendResult appendCommitLog(ByteBuffer message, boolean commit);

    /**
     * Append message to consume queue file, not commit immediately
     *
     * @param request the dispatch request
     * @return append result
     */
    AppendResult appendConsumeQueue(DispatchRequest request);

    /**
     * Append message to consume queue file and commit sync
     *
     * @param request the dispatch request
     * @param commit  whether to commit
     * @return append result
     */
    AppendResult appendConsumeQueue(DispatchRequest request, boolean commit);

    CompletableFuture<ByteBuffer> getMessageAsync(long queueOffset);

    /**
     * Read message from commitlog file at specified offset and length
     *
     * @param offset the offset
     * @param length the length
     * @return the message inner object serialized content
     */
    CompletableFuture<ByteBuffer> readCommitLog(long offset, int length);

    CompletableFuture<ByteBuffer> readConsumeQueue(long queueOffset);

    /**
     * Read messages from consumequeue file at specified offset and count
     *
     * @param queueOffset the message offset
     * @param count       the number of messages to read
     * @return the message body
     */
    CompletableFuture<ByteBuffer> readConsumeQueue(long queueOffset, int count);

    void commitCommitLog();

    void commitConsumeQueue();

    void commit(boolean sync);

    /**
     * Get the maximum offset when building consume queue file
     *
     * @return the maximum offset
     */
    long getBuildCQMaxOffset();

    long binarySearchInQueueByTime(long timestamp, BoundaryType boundaryType);

    void cleanExpiredFile(long expireTimestamp);

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

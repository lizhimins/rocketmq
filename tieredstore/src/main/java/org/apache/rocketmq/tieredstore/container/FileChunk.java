package org.apache.rocketmq.tieredstore.container;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.tieredstore.common.AppendResult;
import org.apache.rocketmq.tieredstore.common.BoundaryType;

interface FileChunk {

    void initOffset(long offset);

    long getBuildCQMaxOffset();

    long binarySearchInQueueByTime(long timestamp, BoundaryType boundaryType);

    AppendResult appendCommitLog(ByteBuffer message);

    AppendResult appendCommitLog(ByteBuffer message, boolean commit);

    AppendResult appendConsumeQueue(DispatchRequest request);

    AppendResult appendConsumeQueue(DispatchRequest request, boolean commit);

    CompletableFuture<ByteBuffer> getMessageAsync(long queueOffset);

    CompletableFuture<ByteBuffer> readCommitLog(long offset, int length);

    CompletableFuture<ByteBuffer> readConsumeQueue(long queueOffset);

    CompletableFuture<ByteBuffer> readConsumeQueue(long queueOffset, int count);

    void commitCommitLog();

    void commitConsumeQueue();

    void cleanExpiredFile(long expireTimestamp);

    void destroyExpiredFile();

    void commit(boolean sync);

    void shutdown();

    void destroy();
}

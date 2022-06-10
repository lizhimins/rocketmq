package org.apache.rocketmq.store.ha.protocol;

import java.nio.ByteBuffer;

public class PushCommitLogData {

    private long epoch;

    private long startOffset;

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset = startOffset;
    }

    public ByteBuffer encode() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(16);
        byteBuffer.putLong(epoch);
        byteBuffer.putLong(startOffset);
        byteBuffer.flip();
        return byteBuffer;
    }

    @Override
    public String toString() {
        return "PushCommitLogData{" +
            "epoch=" + epoch +
            ", startOffset=" + startOffset +
            '}';
    }
}

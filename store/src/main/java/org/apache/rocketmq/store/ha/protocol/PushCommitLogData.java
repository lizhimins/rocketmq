package org.apache.rocketmq.store.ha.protocol;

import java.nio.ByteBuffer;

public class PushCommitLogData {

    private long epoch;

    private long confirmOffset;

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

    public long getConfirmOffset() {
        return confirmOffset;
    }

    public void setConfirmOffset(long confirmOffset) {
        this.confirmOffset = confirmOffset;
    }

    public ByteBuffer encode() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(24);
        byteBuffer.putLong(epoch);
        byteBuffer.putLong(confirmOffset);
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

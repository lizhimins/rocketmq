package org.apache.rocketmq.store.ha.protocol;

import java.util.Arrays;

public class PushCommitLogData {

    private long epoch;

    private long startOffset;

    private int crc32 = 0;

    private byte[] content;

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

    public int getCrc32() {
        return crc32;
    }

    public void setCrc32(int crc32) {
        this.crc32 = crc32;
    }

    public byte[] getContent() {
        return content;
    }

    public void setContent(byte[] content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "PushCommitLogData{" +
            "epoch=" + epoch +
            ", startOffset=" + startOffset +
            ", crc32=" + crc32 +
            ", content=" + Arrays.toString(content) +
            '}';
    }
}

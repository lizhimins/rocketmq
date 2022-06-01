package org.apache.rocketmq.store.ha.protocol;

public class ConfirmTruncate {

    private Long truncateCommitLogOffset;

    public ConfirmTruncate(Long truncateCommitLogOffset) {
        this.truncateCommitLogOffset = truncateCommitLogOffset;
    }

    public Long getTruncateCommitLogOffset() {
        return truncateCommitLogOffset;
    }

    public void setTruncateCommitLogOffset(Long truncateCommitLogOffset) {
        this.truncateCommitLogOffset = truncateCommitLogOffset;
    }

    @Override
    public String toString() {
        return "ConfirmTruncate{" +
            "truncateCommitLogOffset=" + truncateCommitLogOffset +
            '}';
    }
}

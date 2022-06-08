package org.apache.rocketmq.store.ha.protocol;

public class ConfirmTruncate {

    private Long commitLogStartOffset;

    public ConfirmTruncate(Long commitLogStartOffset) {
        this.commitLogStartOffset = commitLogStartOffset;
    }

    public Long getCommitLogStartOffset() {
        return commitLogStartOffset;
    }

    public void setCommitLogStartOffset(Long commitLogStartOffset) {
        this.commitLogStartOffset = commitLogStartOffset;
    }

    @Override
    public String toString() {
        return "ConfirmTruncate{" +
            "truncateCommitLogOffset=" + commitLogStartOffset +
            '}';
    }
}

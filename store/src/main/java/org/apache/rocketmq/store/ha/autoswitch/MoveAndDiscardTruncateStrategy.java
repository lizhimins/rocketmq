package org.apache.rocketmq.store.ha.autoswitch;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.SelectMappedBufferResult;

public class MoveAndDiscardTruncateStrategy implements TruncateStrategy {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    @Override
    public long truncateInvalidMsg(DefaultMessageStore defaultMessageStore, long truncateOffset) {

        long dispatchBehind = defaultMessageStore.dispatchBehindBytes();
        if (dispatchBehind <= 0) {
            LOGGER.info("Dispatch complete, skip truncate");
            return -1;
        }

        long reputFromOffset = defaultMessageStore.getMaxPhyOffset() - dispatchBehind;

        boolean doNext = true;

        while (reputFromOffset < defaultMessageStore.getMaxPhyOffset() && doNext) {
            SelectMappedBufferResult result = defaultMessageStore.getCommitLog().getData(reputFromOffset);
            if (result == null) {
                break;
            }

            try {
                reputFromOffset = result.getStartOffset();

                int readSize = 0;
                while (readSize < result.getSize()) {
                    DispatchRequest dispatchRequest =
                        defaultMessageStore.getCommitLog().checkMessageAndReturnSize(
                            result.getByteBuffer(), false, false);

                    int size = dispatchRequest.getMsgSize();

                    if (dispatchRequest.isSuccess()) {
                        if (size > 0) {
                            reputFromOffset += size;
                            readSize += size;
                        } else {
                            reputFromOffset = defaultMessageStore.getCommitLog().rollNextFile(reputFromOffset);
                            break;
                        }
                    } else {
                        doNext = false;
                        break;
                    }
                }
            } finally {
                result.release();
            }
        }

        LOGGER.info("AutoRecoverHAClient truncate commitLog to {}", reputFromOffset);
        defaultMessageStore.truncateDirtyFiles(reputFromOffset);
        return reputFromOffset;
    }
}

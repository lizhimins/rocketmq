package org.apache.rocketmq.store.ha.file;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.SelectMappedBufferResult;

/**
 * Try to truncate incomplete msg transferred from master.
 */
public class MoveAndDiscardTruncateStrategy implements TruncateStrategy {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    @Override
    public long truncateInvalidMsg(DefaultMessageStore defaultMessageStore, long truncateOffset) {

        LOGGER.info("Start truncate invalid message, message store maxOffset={}, truncateOffset={}",
            defaultMessageStore.getMaxPhyOffset(), truncateOffset);

        boolean doNext = true;
        long dispatchBehindBytes = defaultMessageStore.dispatchBehindBytes();
        long dispatchStartOffset = defaultMessageStore.getMaxPhyOffset() - dispatchBehindBytes;
        while (dispatchStartOffset < defaultMessageStore.getMaxPhyOffset() && doNext) {
            SelectMappedBufferResult result = defaultMessageStore.getCommitLog().getData(dispatchStartOffset);
            if (result == null) {
                break;
            }

            try {
                dispatchStartOffset = result.getStartOffset();
                int readSize = 0;
                while (readSize < result.getSize()) {
                    DispatchRequest dispatchRequest = defaultMessageStore.getCommitLog().checkMessageAndReturnSize(
                        result.getByteBuffer(), false, false);
                    int size = dispatchRequest.getMsgSize();
                    if (dispatchRequest.isSuccess()) {
                        if (size > 0) {
                            dispatchStartOffset += size;
                            readSize += size;
                        } else {
                            dispatchStartOffset = defaultMessageStore.getCommitLog().rollNextFile(dispatchStartOffset);
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

        defaultMessageStore.truncateDirtyFiles(dispatchStartOffset);
        LOGGER.info("Finish truncate commitLog to offset={}", dispatchStartOffset);

        return defaultMessageStore.getMaxPhyOffset();
    }
}

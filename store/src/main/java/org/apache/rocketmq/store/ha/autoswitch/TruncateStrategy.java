package org.apache.rocketmq.store.ha.autoswitch;

import org.apache.rocketmq.store.DefaultMessageStore;

public interface TruncateStrategy {

    long truncateInvalidMsg(DefaultMessageStore defaultMessageStore, long truncateOffset);
}

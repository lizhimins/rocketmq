package org.apache.rocketmq.store.ha.file;

import org.apache.rocketmq.store.DefaultMessageStore;

public interface TruncateStrategy {

    long truncateInvalidMsg(DefaultMessageStore defaultMessageStore);
}

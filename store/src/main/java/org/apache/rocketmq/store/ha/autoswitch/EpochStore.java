package org.apache.rocketmq.store.ha.autoswitch;

import java.util.List;
import org.apache.rocketmq.common.EpochEntry;

public interface EpochStore {

    boolean initStateFromFile();

    void initStateFromEntries(final List<EpochEntry> entries);

    boolean tryAppendEpochEntry(final EpochEntry entry);

    EpochEntry getLastEntry();

    long getLastEpoch();

    EpochEntry findEpochEntryByEpoch(final long epoch);

    EpochEntry findEpochEntryByOffset(final long offset);

    EpochEntry findCeilingEntryByEpoch(final long epoch);

    List<EpochEntry> getAllEntries();

    void truncatePrefixByOffset(final long truncateOffset);

    long findLastConsistentPoint(final EpochStore compareEpoch);

    void truncateSuffixByEpoch(final int truncateEpoch);

    void truncateSuffixByOffset(final long truncateOffset);
}

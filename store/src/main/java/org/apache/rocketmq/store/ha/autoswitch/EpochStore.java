package org.apache.rocketmq.store.ha.autoswitch;

import java.util.List;
import org.apache.rocketmq.common.EpochEntry;

public interface EpochStore {

    boolean initStateFromFile();

    void initStateFromEntries(final List<EpochEntry> entries);

    boolean tryAppendEpochEntry(final EpochEntry entry);

    List<EpochEntry> getAllEntries();

    EpochEntry getLastEntry();

    EpochEntry findEpochEntryByEpoch(final long epoch);

    EpochEntry findEpochEntryByOffset(final long offset);

    EpochEntry findCeilingEntryByEpoch(final long epoch);

    long getLastEpoch();

    /**
     * Find the consistentPoint between compareStore and local.
     */
    long findLastConsistentPoint(final EpochStore compareEpoch);

    /**
     * Remove epochEntries with endOffset <= truncateOffset.
     */
    void truncatePrefixByOffset(final long truncateOffset);

    /**
     * Remove epochEntries with epoch >= truncateEpoch.
     */
    void truncateSuffixByEpoch(final int truncateEpoch);

    /**
     * Remove epochEntries with startOffset >= truncateOffset.
     */
    void truncateSuffixByOffset(final long truncateOffset);
}

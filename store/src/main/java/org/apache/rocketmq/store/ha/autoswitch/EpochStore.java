/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

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

package org.apache.rocketmq.tieredstore.container;

import org.apache.rocketmq.tieredstore.common.FileSegmentType;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.provider.FileSegmentFactory;

public class TieredFileFactory {

    private final FileSegmentFactory fileSegmentFactory;
    private final TieredMessageStoreConfig storeConfig;

    public TieredFileFactory(TieredMessageStoreConfig storeConfig)
        throws ClassNotFoundException, NoSuchMethodException {

        this.storeConfig = storeConfig;
        this.fileSegmentFactory = new FileSegmentFactory(storeConfig);
    }

    public TieredMessageStoreConfig getStoreConfig() {
        return storeConfig;
    }

    public TieredFileQueue createQueueForCommitLog(String filePath) {
        TieredFileQueue tieredFileQueue =
            new TieredFileQueue(fileSegmentFactory, FileSegmentType.COMMIT_LOG, filePath);
        if (tieredFileQueue.getBaseOffset() == -1L) {
            tieredFileQueue.setBaseOffset(0L);
        }
        return tieredFileQueue;
    }

    public TieredFileQueue createQueueForConsumeQueue(String filePath) {
        return new TieredFileQueue(fileSegmentFactory, FileSegmentType.CONSUME_QUEUE, filePath);
    }

    public TieredFileQueue createQueueForIndexFile(String filePath) {
        return new TieredFileQueue(fileSegmentFactory, FileSegmentType.INDEX, filePath);
    }
}

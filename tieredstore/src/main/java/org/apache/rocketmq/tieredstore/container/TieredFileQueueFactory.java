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

import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.provider.FileSegmentFactory;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.apache.rocketmq.tieredstore.util.TieredStoreUtil;

public class TieredFileQueueFactory {

    private static final Logger log = LoggerFactory.getLogger(TieredStoreUtil.TIERED_STORE_LOGGER_NAME);

    private final FileSegmentFactory fileSegmentFactory;
    private final TieredMessageStoreConfig storeConfig;

    public TieredFileQueueFactory(TieredMessageStoreConfig storeConfig)
        throws ClassNotFoundException, NoSuchMethodException {

        this.storeConfig = storeConfig;
        this.fileSegmentFactory = new FileSegmentFactory(storeConfig);
    }

    public TieredCommitLog createTieredStoreCommitLog(String filePath)
        throws ClassNotFoundException, NoSuchMethodException {

        return new TieredCommitLog(fileSegmentFactory, TieredFileSegment.FileSegmentType.COMMIT_LOG, filePath);
    }

    public TieredFileQueue createTieredStoreConsumeQueue(String filePath)
        throws ClassNotFoundException, NoSuchMethodException {

        return new TieredFileQueue(fileSegmentFactory, TieredFileSegment.FileSegmentType.CONSUME_QUEUE, filePath);
    }
}

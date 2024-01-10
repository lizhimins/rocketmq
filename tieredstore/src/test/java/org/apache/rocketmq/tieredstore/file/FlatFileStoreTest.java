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
package org.apache.rocketmq.tieredstore.file;

import java.io.IOException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.MessageStoreConfig;
import org.apache.rocketmq.tieredstore.RemoteMessageStoreTest;
import org.apache.rocketmq.tieredstore.metadata.MetadataStore;
import org.apache.rocketmq.tieredstore.metadata.DefaultMetadataStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FlatFileStoreTest {

    private final String storePath = RemoteMessageStoreTest.getRandomStorePath();
    private MessageStoreConfig storeConfig;
    private MessageQueue mq;
    private MetadataStore metadataStore;

    @Before
    public void setUp() {
        storeConfig = new MessageStoreConfig();
        storeConfig.setStorePathRootDir(storePath);
        storeConfig.setTieredBackendServiceProvider("org.apache.rocketmq.tieredstore.provider.MemoryFileSegment");
        storeConfig.setBrokerName(storeConfig.getBrokerName());
        mq = new MessageQueue("TieredFlatFileManagerTest", storeConfig.getBrokerName(), 0);
        metadataStore = new DefaultMetadataStore(storeConfig);
//        MessageStoreExecutor.init();
    }

    @After
    public void tearDown() throws IOException {
//        MessageStoreTest.deleteStoreDirectory(storePath);
//        MessageStoreExecutor.shutdown();
    }

    @Test
    public void testLoadAndDestroy() throws ClassNotFoundException, NoSuchMethodException {
//        metadataStore.addTopic(mq.getTopic(), 0);
//        metadataStore.addQueue(mq, 100);
//        MessageQueue mq1 = new MessageQueue(mq.getTopic(), mq.getBrokerName(), 1);
//        metadataStore.addQueue(mq1, 200);
//        FlatFileStore flatFileManager = new FlatFileStore(metadataStore, storeConfig);
//        boolean load = flatFileManager.load();
//        Assert.assertTrue(load);
//
//        Awaitility.await()
//            .atMost(3, TimeUnit.SECONDS)
//            .until(() -> flatFileManager.deepCopyFlatFileToList().size() == 2);
//
//        FlatMessageFile flatFile = flatFileManager.getFlatFile(mq);
//        Assert.assertNotNull(flatFile);
//        Assert.assertEquals(-1L, flatFile.getDispatchOffset());
//        flatFile.initOffset(100L);
//        Assert.assertEquals(100L, flatFile.getDispatchOffset());
//        flatFile.initOffset(200L);
//        Assert.assertEquals(100L, flatFile.getDispatchOffset());
//
//        FlatMessageFile flatFile1 = flatFileManager.getFlatFile(mq1);
//        Assert.assertNotNull(flatFile1);
//        flatFile1.initOffset(200L);
//        Assert.assertEquals(200, flatFile1.getDispatchOffset());
//
//        flatFileManager.destroyFile(mq);
//        Assert.assertTrue(flatFile.isClosed());
//        Assert.assertNull(flatFileManager.getFlatFile(mq));
//        Assert.assertNull(metadataStore.getQueue(mq));
//
//        flatFileManager.destroy();
//        Assert.assertTrue(flatFile1.isClosed());
//        Assert.assertNull(flatFileManager.getFlatFile(mq1));
//        Assert.assertNull(metadataStore.getQueue(mq1));
    }
}

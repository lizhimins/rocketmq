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

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MappedFileQueue;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AutoSwitchHATest {

    private static final String TOPIC = "FooBar";
    private static final String GROUP = "GROUP-A";
    private static final String BROKER_NAME = "brokerName";
    private static final String COMMIT_LOG = "commitlog";
    private static final String CHECKPOINT_NAME = "epoch.ckpt";
    private static final int DEFAULT_MAPPED_FILE_SIZE = 1024 * 1024;

    private int queueTotal = 4;
    private final AtomicInteger queueId = new AtomicInteger(0);
    private final String messageBodyString = "Once, there was a chance for me!";
    private final byte[] messageBody = messageBodyString.getBytes();
    private final BrokerStatsManager brokerStatsManager =
        new BrokerStatsManager("HASimpleTest", true);
    private final String storePathRootParentDir =
        Paths.get(System.getProperty("user.home"), "store").toString();
    private final String storePathRootDir =
        Paths.get(storePathRootParentDir, UUID.randomUUID().toString().replace("-", "")).toString();

    private SocketAddress bornHost;
    private SocketAddress storeHost;

    // Broker HA port 7000 7001 7002
    // Broker port    8000 8001 8002
    private DefaultMessageStore messageStore1;
    private DefaultMessageStore messageStore2;
    private DefaultMessageStore messageStore3;

    @Before
    public void initMessageStore() throws UnknownHostException {
        storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 10911);
        bornHost = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    }

    private MessageStoreConfig buildMessageStoreConfig(long brokerId, int mappedFileSize) {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setBrokerRole(BrokerRole.SLAVE);

        String storePath = storePathRootDir + File.separator + BROKER_NAME + "-" + brokerId;
        messageStoreConfig.setStorePathRootDir(storePath);
        messageStoreConfig.setStorePathCommitLog(storePath + File.separator + COMMIT_LOG);
        messageStoreConfig.setStorePathEpochFile(storePath + File.separator + CHECKPOINT_NAME);
        messageStoreConfig.setHaListenPort(7000);
        messageStoreConfig.setTotalReplicas(3);
        messageStoreConfig.setInSyncReplicas(1);

        messageStoreConfig.setMappedFileSizeCommitLog(mappedFileSize);
        messageStoreConfig.setMappedFileSizeConsumeQueue(1024 * 1024);
        messageStoreConfig.setMaxHashSlotNum(10000);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);

        return messageStoreConfig;
    }

    private String getHaAddressFromStore(DefaultMessageStore messageStore) {
        return "127.0.0.1:" + messageStore.getMessageStoreConfig().getHaListenPort();
    }

    private DefaultMessageStore buildMessageStore(MessageStoreConfig messageStoreConfig, long brokerId)
        throws Exception {

        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setBrokerName(BROKER_NAME);
        brokerConfig.setBrokerId(brokerId);
        brokerConfig.setEnableControllerMode(true);

        return new DefaultMessageStore(
            messageStoreConfig, brokerStatsManager, null, brokerConfig);
    }

    private MessageExtBrokerInner buildMessage() {
        MessageExtBrokerInner msg = new MessageExtBrokerInner();
        msg.setTopic(TOPIC);
        msg.setTags("TAG1");
        msg.setBody(messageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(Math.abs(queueId.getAndIncrement()) % queueTotal);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));
        return msg;
    }

    public void initMessageStore(int mappedFileSize) throws Exception {
        MessageStoreConfig storeConfig1 = buildMessageStoreConfig(1, mappedFileSize);
        MessageStoreConfig storeConfig2 = buildMessageStoreConfig(2, mappedFileSize);
        MessageStoreConfig storeConfig3 = buildMessageStoreConfig(3, mappedFileSize);

        storeConfig1.setBrokerRole(BrokerRole.SYNC_MASTER);
        storeConfig1.setHaListenPort(7000);

        storeConfig2.setBrokerRole(BrokerRole.SLAVE);
        storeConfig2.setHaListenPort(7001);

        storeConfig3.setBrokerRole(BrokerRole.SLAVE);
        storeConfig3.setHaListenPort(7002);

        // Elect broker to be master
        messageStore1 = buildMessageStore(storeConfig1, 0L);
        messageStore2 = buildMessageStore(storeConfig2, 1L);
        messageStore3 = buildMessageStore(storeConfig3, 3L);

        ((AutoSwitchHAService) this.messageStore1.getHaService()).setLocalAddress("127.0.0.1:8000");
        ((AutoSwitchHAService) this.messageStore2.getHaService()).setLocalAddress("127.0.0.1:8001");
        ((AutoSwitchHAService) this.messageStore3.getHaService()).setLocalAddress("127.0.0.1:8002");

        assertTrue(messageStore1.load());
        assertTrue(messageStore2.load());
        assertTrue(messageStore3.load());

        messageStore1.start();
        messageStore2.start();
        messageStore3.start();
    }

    @After
    public void destroy() throws Exception {
        System.out.println("================ start destroy ================");
        if (this.messageStore1 != null) {
            messageStore1.shutdown();
            messageStore1.destroy();
        }

        if (this.messageStore2 != null) {
            messageStore2.shutdown();
            messageStore2.destroy();
        }

        if (this.messageStore3 != null) {
            messageStore3.shutdown();
            messageStore3.destroy();
        }

        File file = new File(storePathRootParentDir);
        UtilAll.deleteFile(file);
    }

    private int getMessageCount(final DefaultMessageStore messageStore) {
        return getMessageCount(messageStore, 0);
    }

    private int getMessageCount(final DefaultMessageStore messageStore, int startIndex) {
        int foundMessage = 0;
        for (int i = 0; i < queueTotal; i++) {
            GetMessageResult result = messageStore.getMessage(
                GROUP, TOPIC, i, startIndex, 1024 * 1024, null);
            assertThat(result).isNotNull();
            if (GetMessageStatus.FOUND.equals(result.getStatus())) {
                foundMessage += result.getMessageCount();
            }
            //System.out.print("queueId: " + i + ", message: " + result.getMessageCount() + "\n");
            result.release();
        }
        System.out.printf("min: %d, max: %d, test found message total: %d, confirm offset: %d%n",
            messageStore.getMinPhyOffset(), messageStore.getMaxPhyOffset(), foundMessage,
            ((AutoSwitchHAService) messageStore.getHaService()).getConfirmOffset());
        return foundMessage;
    }

    @Test
    public void testTransferMessage() throws Exception {
        initMessageStore(DEFAULT_MAPPED_FILE_SIZE);
        messageStore1.getHaService().changeToMaster(1);
        messageStore2.getHaService().changeToSlave("", 1, 2L);
        messageStore2.getHaService().updateHaMasterAddress(getHaAddressFromStore(messageStore1));

        int messageCount = 100;
        for (int i = 0; i < messageCount; i++) {
            messageStore1.putMessage(buildMessage());
        }

        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(
            () -> messageCount == getMessageCount(messageStore1));

        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(
            () -> messageCount == getMessageCount(messageStore2));
    }

    @Test
    public void testAsyncLearnerBrokerRole() throws Exception {
        initMessageStore(DEFAULT_MAPPED_FILE_SIZE);

        messageStore1.getHaService().changeToMaster(1);
        messageStore2.getHaService().changeToSlave("", 1, 2L);
        messageStore2.getHaService().updateHaMasterAddress(getHaAddressFromStore(messageStore1));

        int messageCount = 100;
        for (int i = 0; i < messageCount; i++) {
            messageStore1.putMessage(buildMessage());
        }

        await().atMost(Duration.ofSeconds(30)).until(
            () -> messageCount == getMessageCount(messageStore1));

        await().atMost(Duration.ofSeconds(30)).until(
            () -> messageCount == getMessageCount(messageStore2));

        messageStore2.getMessageStoreConfig().setHaSendHeartbeatInterval(1000);
        messageStore2.getMessageStoreConfig().setAsyncLearner(false);
        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(() -> {
            final Set<String> syncStateSet =
                ((AutoSwitchHAService) this.messageStore1.getHaService()).getSyncStateSet();
            return syncStateSet.size() == 2 && syncStateSet.contains("127.0.0.1:8001");
        });

        messageStore2.getMessageStoreConfig().setAsyncLearner(true);
        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(() -> {
            final Set<String> syncStateSet =
                ((AutoSwitchHAService) this.messageStore1.getHaService()).getSyncStateSet();
            return syncStateSet.size() == 1 && syncStateSet.contains("127.0.0.1:8000");
        });

        messageStore2.getMessageStoreConfig().setAsyncLearner(false);
        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(() -> {
            final Set<String> syncStateSet =
                ((AutoSwitchHAService) this.messageStore1.getHaService()).getSyncStateSet();
            return syncStateSet.size() == 2 && syncStateSet.contains("127.0.0.1:8001");
        });
    }

    private void changeMasterAndPutMessage(int epoch, DefaultMessageStore masterStore, String masterHaAddress,
        DefaultMessageStore slaveStore, long slaveId, int totalPutMessageNums) {

        // Async add slave
        slaveStore.getBrokerConfig().setBrokerId(slaveId);
        slaveStore.getMessageStoreConfig().setBrokerRole(BrokerRole.SLAVE);
        slaveStore.getHaService().changeToSlave("", epoch, slaveId);
        slaveStore.getHaService().updateHaMasterAddress(masterHaAddress);

        masterStore.getMessageStoreConfig().setHaMasterAddress(masterHaAddress);
        masterStore.getBrokerConfig().setBrokerId(MixAll.MASTER_ID);
        masterStore.getMessageStoreConfig().setBrokerRole(BrokerRole.SYNC_MASTER);
        masterStore.getHaService().changeToMaster(epoch);

        // Put message on master
        for (int i = 0; i < totalPutMessageNums; i++) {
            masterStore.putMessage(buildMessage());
        }
    }

    @Test
    public void testOptionAllAckInSyncStateSet() throws Exception {
        initMessageStore(DEFAULT_MAPPED_FILE_SIZE);

        int messageCount = 10;
        changeMasterAndPutMessage(1, this.messageStore1, "127.0.0.1:7000",
            this.messageStore2, 1, messageCount);
        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(
            () -> messageCount == getMessageCount(messageStore2));

        // At least need master and another replica ack
        this.messageStore1.getMessageStoreConfig().setInSyncReplicas(2);

        // Put message on master
        for (int i = 0; i < messageCount; i++) {
            PutMessageResult result = this.messageStore1.putMessage(buildMessage());
            assertEquals(result.getPutMessageStatus(), PutMessageStatus.PUT_OK);
        }

        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(
            () -> messageCount * 2 == getMessageCount(messageStore2));

        long maxPhyOffset = this.messageStore1.getMaxPhyOffset();
        long confirmOffset = ((AutoSwitchHAService) messageStore2.getHaService()).computeConfirmOffset();
        assertEquals(maxPhyOffset, confirmOffset);

        // Now, shutdown store2
        this.messageStore2.shutdown();
        this.messageStore2.destroy();

        // Force reset in sync state set to store1 and store2
        ((AutoSwitchHAService) this.messageStore1.getHaService())
            .setSyncStateSet(new HashSet<>(Arrays.asList("127.0.0.1:8000", "127.0.0.1:8001")));

        final PutMessageResult putMessageResult = this.messageStore1.putMessage(buildMessage());
        assertEquals(putMessageResult.getPutMessageStatus(), PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
    }

    @Test
    public void testMasterRoleNotChange() throws Exception {
        initMessageStore(DEFAULT_MAPPED_FILE_SIZE);
        int messageCount = 10;

        // Step1, change store1 to master, store2 to follower
        changeMasterAndPutMessage(1, this.messageStore1, "127.0.0.1:7000",
            this.messageStore2, 1, messageCount);
        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(
            () -> messageCount == getMessageCount(messageStore2));

        // Step2, role not change
        changeMasterAndPutMessage(2, this.messageStore1, "127.0.0.1:7000",
            this.messageStore2, 1, messageCount);
        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(
            () -> messageCount * 2 == getMessageCount(messageStore2));
    }

    @Test
    public void testChangeRoleManyTimes() throws Exception {
        initMessageStore(DEFAULT_MAPPED_FILE_SIZE);
        int messageCount = 10;

        // Step1, change store1 to master, store2 to follower
        changeMasterAndPutMessage(1, this.messageStore1, "127.0.0.1:7000",
            this.messageStore2, 1, messageCount);
        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(
            () -> messageCount == getMessageCount(messageStore2));

        System.out.println("==========================================");
        // Step2, change store2 to master, epoch = 2
        changeMasterAndPutMessage(2, this.messageStore2, "127.0.0.1:7001",
            this.messageStore1, 1, messageCount);
        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(
            () -> messageCount * 2 == getMessageCount(messageStore1));

        System.out.println("==========================================");
        // Step3, change store1 to master, epoch = 3
        changeMasterAndPutMessage(3, this.messageStore1, "127.0.0.1:7000",
            this.messageStore2, 1, messageCount);
        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(60)).until(
            () -> messageCount * 3 == getMessageCount(messageStore2));
    }

    @Test
    public void testPushDataForDifferentEpoch() throws Exception {
        initMessageStore(1700);

        int times = 5;
        int messageCount = 10;

        for (int i = 0; i < 5; i++) {
            changeMasterAndPutMessage(i, this.messageStore1,
                "127.0.0.1:7000", this.messageStore2, 1, messageCount);
        }

        await().atMost(Duration.ofSeconds(30)).until(
            () -> messageCount * times == getMessageCount(messageStore2));
    }

    @Test
    public void testDynamicAddBroker() throws Exception {
        initMessageStore(DEFAULT_MAPPED_FILE_SIZE);

        int messageCount = 10;
        changeMasterAndPutMessage(1, this.messageStore1, "127.0.0.1:7000",
            this.messageStore2, 2, messageCount);

        await().atMost(Duration.ofSeconds(30)).until(
            () -> messageCount == getMessageCount(messageStore2));

        // Step2: add new broker3, link to broker1
        messageStore3.getHaService().changeToSlave("", 1, 3L);
        messageStore3.getHaService().updateHaMasterAddress("127.0.0.1:7000");

        System.out.println("==========================================33");
        await().atMost(Duration.ofSeconds(30)).until(
            () -> messageCount == getMessageCount(messageStore3));
    }

    @Test
    public void testTruncateCommitLogAndAddBroker() throws Exception {

        queueTotal = 1;

        // Noted that 10 msg 's total size = 1570
        // Init the mappedFileSize = 1700, one file only be used to store 10 msg.
        initMessageStore(1700);
        int messageCount = 10;

        // Step1: broker1 as leader, broker2 as follower. Append epoch 2, each epoch will be stored on one file
        // Master: <Epoch1, 0, 1570> <Epoch2, 1570, 3270>
        changeMasterAndPutMessage(1, this.messageStore1, "127.0.0.1:7000", this.messageStore2, 1, 10);
        changeMasterAndPutMessage(2, this.messageStore1, "127.0.0.1:7000", this.messageStore2, 1, 10);
        await().atMost(Duration.ofSeconds(30)).until(
            () -> messageCount * 2 == getMessageCount(messageStore2));

        // Step2: check file position, each epoch will be stored on one file
        // So epoch1 was stored in firstFile, epoch2 was stored in second file, the lastFile was empty.
        final MappedFileQueue fileQueue = this.messageStore1.getCommitLog().getMappedFileQueue();
        assertEquals(2, fileQueue.getTotalFileSize() / 1700);

        // Step3: truncate epoch1's log (truncateEndOffset = 1570), which means we should delete the first file directly.
        final MappedFile firstFile = this.messageStore1.getCommitLog().getMappedFileQueue().getFirstMappedFile();
        firstFile.shutdown(1000);
        fileQueue.retryDeleteFirstFile(1000);
        assertEquals(this.messageStore1.getCommitLog().getMinOffset(), 1700);

        final AutoSwitchHAService haService = (AutoSwitchHAService) this.messageStore1.getHaService();
        haService.truncateEpochFilePrefix(1570);
        assertEquals(1, haService.getEpochEntries().size());

        ((ConsumeQueue) this.messageStore1.getConsumeQueue(TOPIC, 0)).correctMinOffset(1570);

        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(
            () -> 10 == getMessageCount(messageStore1, 10));

        System.out.println("==============================");
        // Step4: add broker3 as slave, only have 10 msg from offset 10, broker3 copy from first file
        messageStore3.getHaService().changeToSlave("", 2, 3L);
        messageStore3.getHaService().updateHaMasterAddress("127.0.0.1:7000");

        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(
            () -> 10 == getMessageCount(messageStore3, 10));

        System.out.println("1213123123123");
        System.out.println(((AutoSwitchHAService) messageStore3.getHaService()).getEpochEntries());
    }

    @Test
    public void testTruncateEpochLogAndChangeMaster() throws Exception {
        testTruncateCommitLogAndAddBroker();

        // Step5: change broker2 as leader, broker3 as follower

        // store1: <Epoch1, 0, 1570> <Epoch2, 1570, 3270>
        // store2: <Epoch1, 0, 1570> <Epoch2, 1570, 3270>

        // store1:                   <Epoch2, 1570, 3270>
        // store2: <Epoch1, 0, 1570> <Epoch2, 1570, 3270> <Epoch3, 3400, 4970>
        // store3:                   <Epoch2, 1570, 3270> <Epoch3, 3400, 4970>

        System.out.println("=====================");
        changeMasterAndPutMessage(3, this.messageStore2, "127.0.0.1:7001", this.messageStore3, 3, 10);
        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(
            () -> 30 == getMessageCount(messageStore2, 0));

        ConsumeQueueInterface consumeQueue = messageStore3.getConsumeQueue(TOPIC, 0);
        System.out.println(consumeQueue.getMinOffsetInQueue() + " " + consumeQueue.getMaxOffsetInQueue());

        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(
            () -> 20 == getMessageCount(messageStore3, 10));

        //System.out.println(((AutoSwitchHAService) messageStore3.getHaService()).getEpochEntries().toString());
        //System.out.println(messageStore3.getMinPhyOffset());
        //System.out.println(messageStore3.getMaxPhyOffset());

        messageStore3.shutdown();
        messageStore3.destroy();

        // Step6, let broker1 link to broker2, it should sync log from epoch3.
        this.messageStore1.getBrokerConfig().setBrokerId(1);
        this.messageStore1.getMessageStoreConfig().setBrokerRole(BrokerRole.SLAVE);
        this.messageStore1.getHaService().changeToSlave("", 3, 1L);
        this.messageStore1.getHaService().updateHaMasterAddress("127.0.0.1:7001");

        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(
            () -> 20 == getMessageCount(messageStore1, 10));
    }

    @Test
    public void testAddBrokerAndSyncFromLastFile() throws Exception {
        initMessageStore(1700);
        queueTotal = 1;

        // Step1: broker1 as leader, broker2 as follower
        // append epoch 2, each epoch will be stored on one file
        // Master: <Epoch1, 0, 1570> <Epoch2, 1570, 3270>
        int messageCount = 10;
        changeMasterAndPutMessage(1, this.messageStore1, "127.0.0.1:7000", this.messageStore2, 1, messageCount);
        changeMasterAndPutMessage(2, this.messageStore1, "127.0.0.1:7000", this.messageStore2, 1, messageCount);
        await().atMost(Duration.ofSeconds(30)).until(
            () -> messageCount * 2 == getMessageCount(messageStore2));

        messageStore2.shutdown();
        messageStore2.destroy();

        // Step2: restart broker3
        messageStore3.shutdown();
        messageStore3.destroy();

        messageStore3.getMessageStoreConfig().setSyncFromLastFile(true);
        messageStore3 = buildMessageStore(messageStore3.getMessageStoreConfig(), 3L);
        assertTrue(messageStore3.load());
        messageStore3.start();

        System.out.println("========================================");
        // Step3: add new broker3, link to broker1.
        // Due to broker3 request sync from lastFile, so it only synced 10 msg from offset 10;
        messageStore3.getHaService().changeToSlave("", 2, 3L);
        messageStore3.getHaService().updateHaMasterAddress("127.0.0.1:7000");

        TimeUnit.SECONDS.sleep(6);
        // Put message on master
        for (int i = 0; i < messageCount; i++) {
            messageStore1.putMessage(buildMessage());
        }

        // Sync from last file, but not start from mapped file first
        // Message total count is 20
        await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30)).until(
            () -> 20 == getMessageCount(messageStore3, 10));
    }
}

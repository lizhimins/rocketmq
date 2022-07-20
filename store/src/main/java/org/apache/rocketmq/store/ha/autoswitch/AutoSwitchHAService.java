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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.GroupTransferService;
import org.apache.rocketmq.store.ha.HAClient;
import org.apache.rocketmq.store.ha.HAConnection;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.apache.rocketmq.store.ha.HAConnectionStateNotificationRequest;
import org.apache.rocketmq.store.ha.HAConnectionStateNotificationService;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.ha.WaitNotifyObject;
import org.apache.rocketmq.store.ha.file.MoveAndDiscardTruncateStrategy;
import org.apache.rocketmq.store.ha.file.TruncateStrategy;
import org.apache.rocketmq.store.ha.netty.NettyHADecoder;
import org.apache.rocketmq.store.ha.netty.NettyHAEncoder;
import org.apache.rocketmq.store.ha.netty.NettyHAServerHandler;
import org.apache.rocketmq.store.ha.protocol.HandshakeMaster;
import org.apache.rocketmq.store.ha.protocol.HandshakeResult;
import org.apache.rocketmq.store.ha.protocol.HandshakeSlave;
import org.apache.rocketmq.store.ha.protocol.PushCommitLogAck;

/**
 * SwitchAble ha service, support switch role to master or slave.
 */
public class AutoSwitchHAService implements HAService {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final int READ_MAX_BUFFER_SIZE = 4 * 1024 * 1024;
    private static final int WRITE_MAX_BUFFER_SIZE = 16 * 1024 * 1024;

    private final ScheduledExecutorService scheduledService = Executors.newScheduledThreadPool(
        1, new ThreadFactoryImpl("ReplicasManager_ExecutorService_"));
    private final ExecutorService executorService = Executors.newSingleThreadExecutor(
        new ThreadFactoryImpl("NettyHAService_Executor_"));

    private final Map<Channel, HAConnection> connectionMap = new ConcurrentHashMap<>();
    private final List<Consumer<Set<String>>> syncStateSetChangedListeners = new ArrayList<>();
    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    private final TruncateStrategy truncateStrategy = new MoveAndDiscardTruncateStrategy();
    private final EventLoopGroup bossGroup = new NioEventLoopGroup(2);
    private final EventLoopGroup workerGroup = new NioEventLoopGroup(2);

    private DefaultMessageStore defaultMessageStore;
    private GroupTransferService groupTransferService;
    private CopyOnWriteArraySet<String> syncStateSet = new CopyOnWriteArraySet<>();
    private HAConnectionStateNotificationService haConnectionStateNotificationService;

    private ChannelFuture future;
    private EpochStore epochCache;
    private long currentMasterEpoch = -1L;
    private long confirmOffset = -1L;
    private String localAddress;
    private AutoSwitchHAClient haClient;

    public AutoSwitchHAService() {
    }

    @Override
    public void init(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.epochCache = new EpochStoreService(defaultMessageStore.getMessageStoreConfig().getStorePathEpochFile());
        this.epochCache.initStateFromFile();
        this.defaultMessageStore = defaultMessageStore;
        this.groupTransferService = new GroupTransferService(this, defaultMessageStore);
        this.haConnectionStateNotificationService =
            new HAConnectionStateNotificationService(this, defaultMessageStore);
    }

    @Override
    public void start() throws Exception {
        this.groupTransferService.start();
        startNettyServer(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
        this.scheduledService.scheduleAtFixedRate(() -> {
            this.setSyncStateSet(this.buildShrinkInSyncStateSet());
        }, 3 * 1000, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        if (this.haClient != null) {
            this.haClient.shutdown();
        }

        executorService.shutdown();
        scheduledService.shutdown();

        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    public void startNettyServer(int port) {
        AutoSwitchHAService haService = this;
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.SO_SNDBUF, WRITE_MAX_BUFFER_SIZE)
            .childOption(ChannelOption.SO_RCVBUF, READ_MAX_BUFFER_SIZE)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel channel) {
                    channel.pipeline()
                        .addLast("decoder", new NettyHADecoder())
                        .addLast("encoder", new NettyHAEncoder())
                        .addLast(new NettyHAServerHandler(haService));
                }
            });

        future = bootstrap.bind(port).addListener((ChannelFutureListener) future1 -> {
            if (future1.isSuccess()) {
                LOGGER.info("Netty HAService start listen at " + port);
                System.out.println("Netty HAService start listen at " + port);
            } else {
                System.out.println("start failed");
            }
        });

        try {
            future.sync();
        } catch (InterruptedException e1) {
            throw new RuntimeException("HAService start server InterruptedException", e1);
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public void registerSyncStateSetChangedListener(final Consumer<Set<String>> listener) {
        this.syncStateSetChangedListeners.add(listener);
    }

    public void notifySyncStateSetChanged(final Set<String> newSyncStateSet) {
        this.executorService.submit(() -> {
            for (Consumer<Set<String>> listener : syncStateSetChangedListeners) {
                listener.accept(newSyncStateSet);
            }
        });
    }

    private void destroyConnections() {
        for (Channel channel : this.connectionMap.keySet()) {
            HAConnection haConnection = this.connectionMap.remove(channel);
            haConnection.shutdown();
        }
    }

    @Override
    public boolean changeToMaster(int masterEpoch) {
        // Not allow elect unclean master
        if (masterEpoch < this.epochCache.getLastEpoch()) {
            System.out.println("failed");
            LOGGER.error("Broker change to master failed, not allow elect unclean master, oldEpoch:{}, newEpoch:{}",
                masterEpoch, this.epochCache.getLastEpoch());
            return false;
        }

        // Remove all slave connections
        destroyConnections();

        // Stop ha client if needed
        if (this.haClient != null) {
            this.haClient.shutdown();
        }

        long maxPhyOffset = this.defaultMessageStore.getMaxPhyOffset();
        long minPhyOffset = this.defaultMessageStore.getMinPhyOffset();

        // Master truncate dirty file
        final long truncateSize = truncateStrategy.truncateInvalidMsg(defaultMessageStore, maxPhyOffset);
        LOGGER.info("Broker truncate msg file, store minPhyOffset:{}, maxPhyOffset:{}, " +
            "newMasterEpoch:{}, truncate size:{}", minPhyOffset, maxPhyOffset, masterEpoch, truncateSize);

        // Correct epoch store
        this.epochCache.truncateSuffixByEpoch(masterEpoch);
        this.epochCache.truncateSuffixByOffset(maxPhyOffset);
        this.epochCache.tryAppendEpochEntry(new EpochEntry(masterEpoch, maxPhyOffset));

        this.currentMasterEpoch = masterEpoch;

        // Rollback index
        this.defaultMessageStore.recoverTopicQueueTable();

        setSyncStateSet(new HashSet<>(Collections.singletonList(this.localAddress)));
        notifySyncStateSetChanged(syncStateSet);

        LOGGER.info("Broker change to master success, newMasterEpoch:{}, startPhyOffset:{}", masterEpoch, maxPhyOffset);
        return true;
    }

    @Override
    public boolean changeToSlave(String newMasterAddr, int newMasterEpoch, Long slaveId) {
        try {
            destroyConnections();
            setSyncStateSet(new HashSet<>());

            if (this.haClient == null) {
                this.haClient = new AutoSwitchHAClient(this);
            } else {
                this.haClient.shutdown();
            }

            this.currentMasterEpoch = newMasterEpoch;

            this.haClient.init();
            this.haClient.updateSlaveId(slaveId);
            this.haClient.updateHaMasterAddress(newMasterAddr);
            this.haClient.setCurrentMasterEpoch(newMasterEpoch);
            this.haClient.start();

            LOGGER.info("Broker change to slave success, newMasterAddress:{}, newMasterEpoch:{}, " +
                "self brokerId:{}", newMasterAddr, newMasterEpoch, slaveId);
            return true;
        } catch (Exception e) {
            LOGGER.error("Error happen when change ha to slave", e);
        }
        return false;
    }

    public long getCurrentMasterEpoch() {
        return currentMasterEpoch;
    }

    @Override
    public HAClient getHAClient() {
        return this.haClient;
    }

    @Override
    public AtomicLong getPush2SlaveMaxOffset() {
        return new AtomicLong(this.confirmOffset);
    }

    /**
     * Get confirm offset (min slaveAckOffset of all syncStateSet members) for master
     */
    public long getConfirmOffset() {
        if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE) {
            if (this.syncStateSet.size() == 1) {
                return this.defaultMessageStore.getMaxPhyOffset();
            }
            // First time compute confirmOffset.
            if (this.confirmOffset <= 0) {
                this.confirmOffset = computeConfirmOffset();
            }
        }
        return confirmOffset;
    }

    @Override
    public HARuntimeInfo getRuntimeInfo(long masterPutWhere) {
        return null;
    }

    @Override
    public WaitNotifyObject getWaitNotifyObject() {
        return this.waitNotifyObject;
    }

    @Override
    public boolean isSlaveOK(long masterPutWhere) {
        return false;
    }

    @Override
    public void updateMasterAddress(String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateHaMasterAddress(newAddr);
        }
    }

    @Override
    public int inSyncReplicasNums(final long masterPutWhere) {
        return syncStateSet.size();
    }

    protected boolean isInSyncSlave(final long masterPutWhere, HAConnection conn) {
        return masterPutWhere - conn.getSlaveAckOffset() <
            this.defaultMessageStore.getMessageStoreConfig().getHaMaxGapNotInSync();
    }

    @Override
    public AtomicInteger getConnectionCount() {
        return new AtomicInteger(this.connectionMap.size());
    }

    @Override
    public void putRequest(CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    @Override
    public void putGroupConnectionStateRequest(HAConnectionStateNotificationRequest request) {
        this.haConnectionStateNotificationService.setRequest(request);
    }

    @Override
    public List<HAConnection> getConnectionList() {
        return new ArrayList<>(this.connectionMap.values());
    }

    /**
     * Check and maybe shrink the inSyncStateSet.
     * A slave will be removed from inSyncStateSet if
     * (curTime - HaConnection.lastCaughtUpTime) > option(haMaxTimeSlaveNotCatchup)
     */
    public synchronized Set<String> buildShrinkInSyncStateSet() {
        final HashSet<String> newSyncStateSet = new HashSet<>();
        final long haMaxTimeSlaveNotCatchup =
            this.defaultMessageStore.getMessageStoreConfig().getHaMaxTimeSlaveNotCatchup();
        for (HAConnection haConnection : this.connectionMap.values()) {
            final AutoSwitchHAConnection connection = (AutoSwitchHAConnection) haConnection;
            final String slaveAddress = connection.getSlaveAddress();
            final boolean firstInit = connection.getSlaveAckOffset() < 0;
            final boolean asyncLearner = connection.isSlaveAsyncLearner();
            final boolean slaveNotCatchup =
                (System.currentTimeMillis() - connection.getSlaveAckTimestamp()) > haMaxTimeSlaveNotCatchup;
            if (firstInit || asyncLearner || slaveNotCatchup) {
                continue;
            }
            newSyncStateSet.add(slaveAddress);
        }
        newSyncStateSet.add(this.localAddress);
        return newSyncStateSet;
    }

    /**
     * Check and maybe add the slave to inSyncStateSet.
     * A slave will be added to inSyncStateSet if its slaveAckOffset >= current confirmOffset
     * and it is caught up to an offset within the current leader epoch.
     */
    public synchronized void tryExpandInSyncStateSet(HAConnection haConnection, final long slaveAckOffset) {
        AutoSwitchHAConnection connection = (AutoSwitchHAConnection) haConnection;

        final Set<String> currentSyncStateSet = getSyncStateSet();
        String brokerAddr = connection.getSlaveAddress();
        if (brokerAddr == null || currentSyncStateSet.contains(brokerAddr)) {
            return;
        }

        final long confirmOffset = getConfirmOffset();
        final EpochEntry currentLeaderEpoch = this.epochCache.getLastEntry();
        if (slaveAckOffset >= confirmOffset && slaveAckOffset >= currentLeaderEpoch.getStartOffset()) {
            currentSyncStateSet.add(brokerAddr);
            setSyncStateSet(currentSyncStateSet);
            // Notify the upper layer that syncStateSet changed.
            notifySyncStateSetChanged(currentSyncStateSet);
            System.out.printf("expand in sync state set %s, slaveAckOffset %d%n",
                currentSyncStateSet, slaveAckOffset);
        }
    }

    public void updateConfirmOffset(long confirmOffset) {
        this.confirmOffset = confirmOffset;
    }

    /**
     * Get confirm offset (min slaveAckOffset of all syncStateSet members)
     */
    public long computeConfirmOffset() {
        final Set<String> currentSyncStateSet = getSyncStateSet();
        long confirmOffset = this.defaultMessageStore.getMaxPhyOffset();

        StringBuilder printData = new StringBuilder(confirmOffset + " ");
        for (HAConnection connection : this.connectionMap.values()) {
            final String slaveAddress = ((AutoSwitchHAConnection) connection).getSlaveAddress();
            if (currentSyncStateSet.contains(slaveAddress)) {
                confirmOffset = Math.min(confirmOffset, connection.getSlaveAckOffset());
                printData.append(connection.getSlaveAckOffset()).append(" ");
            }
        }
        //System.out.println("compute: " + printData);
        return confirmOffset;
    }

    public synchronized void setSyncStateSet(final Set<String> syncStateSet) {
        this.syncStateSet = new CopyOnWriteArraySet<>(syncStateSet);
    }

    public synchronized Set<String> getSyncStateSet() {
        return new HashSet<>(this.syncStateSet);
    }

    public void truncateEpochFilePrefix(final long offset) {
        this.epochCache.truncatePrefixByOffset(offset);
    }

    public void truncateEpochFileSuffix(final long offset) {
        this.epochCache.truncateSuffixByOffset(offset);
    }

    public String getLocalAddress() {
        return localAddress;
    }

    public void setLocalAddress(String localAddress) {
        this.localAddress = localAddress;
    }

    public List<EpochEntry> getEpochEntries() {
        return this.epochCache.getAllEntries();
    }

    public HandshakeResult verifySlaveIdentity(HandshakeSlave handshakeSlave) {
        BrokerConfig brokerConfig = defaultMessageStore.getBrokerConfig();

        if (!StringUtils.equals(brokerConfig.getBrokerClusterName(), handshakeSlave.getClusterName())) {
            return HandshakeResult.CLUSTER_NAME_NOT_MATCH;
        }

        if (!StringUtils.equals(brokerConfig.getBrokerName(), handshakeSlave.getBrokerName())) {
            return HandshakeResult.BROKER_NAME_NOT_MATCH;
        }

        if (handshakeSlave.getBrokerId() == 0L) {
            return HandshakeResult.BROKER_ID_ERROR;
        }

//        if (MQVersion.Version.V5_0_0_SNAPSHOT.ordinal() <= handshakeSlave.getBrokerAppVersion()) {
//            return HandshakeResult.PROTOCOL_NOT_SUPPORT;
//        }

        return HandshakeResult.ACCEPT;
    }

    public HandshakeMaster buildHandshakeResult(HandshakeResult handshakeResult) {
        BrokerConfig brokerConfig = defaultMessageStore.getBrokerConfig();
        HandshakeMaster handshakeMaster = new HandshakeMaster();
        handshakeMaster.setClusterName(brokerConfig.getBrokerClusterName());
        handshakeMaster.setBrokerName(brokerConfig.getBrokerName());
        handshakeMaster.setBrokerId(brokerConfig.getBrokerId());
        handshakeMaster.setBrokerAppVersion(MQVersion.CURRENT_VERSION);
        handshakeMaster.setLanguageCode(LanguageCode.JAVA);
        handshakeMaster.setHaProtocolVersion(2);
        handshakeMaster.setHandshakeResult(handshakeResult);
        return handshakeMaster;
    }

    public void confirmTruncate(Channel channel, long slaveOffset) {
        HAConnection haConnection = this.connectionMap.get(channel);
        if (haConnection != null) {
            long transferStart = slaveOffset;
            long phyMinOffset = defaultMessageStore.getCommitLog().getMinOffset();
            long phyMaxOffset = defaultMessageStore.getCommitLog().getMaxOffset();

            if (transferStart < phyMinOffset) {
                transferStart = phyMinOffset;
            }

            if (transferStart > phyMaxOffset) {
                transferStart = phyMaxOffset;
            }

            // We must ensure that the starting point of syncing log
            // must be the startOffset of a file (maybe the last file, or the minOffset)
            final MessageStoreConfig config = this.defaultMessageStore.getMessageStoreConfig();
            transferStart = transferStart - (transferStart % config.getMappedFileSizeCommitLog());
            if (transferStart < 0) {
                transferStart = 0;
            }

            System.out.println("receive client confirm truncate, start offset " + slaveOffset);
            LOGGER.info("receive client confirm truncate, start offset:{}, start:{}", slaveOffset, transferStart);

            ((AutoSwitchHAConnection) haConnection).setCurrentTransferOffset(transferStart);
            ((AutoSwitchHAConnection) haConnection).changeCurrentState(HAConnectionState.TRANSFER);
        } else {
            LOGGER.error("receive client confirm truncate error, connection lost");
        }
    }

    public void notifyTransferSome() {
        this.confirmOffset = computeConfirmOffset();
        this.groupTransferService.notifyTransferSome();
    }

    public void pushCommitLogDataAck(Channel channel, PushCommitLogAck pushCommitLogAck) {
        AutoSwitchHAConnection haConnection = (AutoSwitchHAConnection) this.connectionMap.get(channel);
        if (haConnection != null) {
            long offset = pushCommitLogAck.getConfirmOffset();
            haConnection.setSlaveAsyncLearner(pushCommitLogAck.isReadOnly());
            haConnection.updateSlaveTransferProgress(offset);
            if (!pushCommitLogAck.isReadOnly()) {
                tryExpandInSyncStateSet(haConnection, offset);
                notifyTransferSome();
            }
        }
    }

    public void tryAcceptNewSlave(Channel channel, HandshakeSlave handshakeSlave) {
        AutoSwitchHAConnection haConnection = new AutoSwitchHAConnection(this, channel, epochCache);
        haConnection.setSlaveId(handshakeSlave.getBrokerId());
        haConnection.setSlaveAddress(handshakeSlave.getBrokerAddr());
        haConnection.start();
        this.addConnection(channel, haConnection);
    }

    public void addConnection(Channel channel, final HAConnection conn) {
        //System.out.println("add connection: " + conn.getClientAddress() + " now: " + (connectionMap.size() + 1));
        this.connectionMap.put(channel, conn);
    }

    public void removeConnection(Channel channel) {
        HAConnection haConnection = this.connectionMap.remove(channel);
        //this.haConnectionStateNotificationService.checkConnectionStateAndNotify(haConnection);
        haConnection.shutdown();
    }

    public EpochStore getEpochCache() {
        return epochCache;
    }

    public void setEpochCache(EpochStore epochCache) {
        this.epochCache = epochCache;
    }
}

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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.ha.GroupTransferService;
import org.apache.rocketmq.store.ha.HAClient;
import org.apache.rocketmq.store.ha.HAConnection;
import org.apache.rocketmq.store.ha.HAConnectionStateNotificationRequest;
import org.apache.rocketmq.store.ha.HAConnectionStateNotificationService;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.ha.WaitNotifyObject;
import org.apache.rocketmq.store.ha.netty.NettyHADecoder;
import org.apache.rocketmq.store.ha.netty.NettyHAEncoder;
import org.apache.rocketmq.store.ha.netty.NettyHAServerHandler;

/**
 * SwitchAble ha service, support switch role to master or slave.
 */
public class AutoSwitchHAService implements HAService {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final int READ_MAX_BUFFER_SIZE = 4 * 1024 * 1024;
    private static final int WRITE_MAX_BUFFER_SIZE = 16 * 1024 * 1024;

    private final ExecutorService executorService =
        Executors.newSingleThreadExecutor(new ThreadFactoryImpl("AutoSwitchHAService_Executor_"));

    private final List<HAConnection> connectionList = new LinkedList<>();
    private final List<Consumer<Set<String>>> syncStateSetChangedListeners = new ArrayList<>();
    private final CopyOnWriteArraySet<String> syncStateSet = new CopyOnWriteArraySet<>();

    private DefaultMessageStore defaultMessageStore;

    private String localAddress;

    private WaitNotifyObject waitNotifyObject = new WaitNotifyObject();
    private AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    private GroupTransferService groupTransferService;

    private HAConnectionStateNotificationService haConnectionStateNotificationService;

    private final TruncateStrategy truncateStrategy = new MoveAndDiscardTruncateStrategy();

    private EpochStore epochCache;

    private final EventLoopGroup bossGroup = new NioEventLoopGroup();
    private final EventLoopGroup workerGroup = new NioEventLoopGroup(2);
    private AutoSwitchHAClient haClient;

    public AutoSwitchHAService() {
    }

    @Override
    public void init(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.epochCache = new EpochFileStore(defaultMessageStore.getMessageStoreConfig().getStorePathEpochFile());
        this.epochCache.initStateFromFile();
        this.defaultMessageStore = defaultMessageStore;
        this.groupTransferService = new GroupTransferService(this, defaultMessageStore);
        this.haConnectionStateNotificationService =
            new HAConnectionStateNotificationService(this, defaultMessageStore);
    }

    @Override
    public void start() throws Exception {
        startNettyServer(defaultMessageStore.getMessageStoreConfig().getHaListenPort());
    }

    @Override
    public void shutdown() {
        if (this.haClient != null) {
            this.haClient.shutdown();
        }
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        this.executorService.shutdown();
    }

    public void startNettyServer(int port) {
        LOGGER.info("Server start listen at " + port);
        System.out.println("server start listen at " + port);
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup(2);

        NettyHAServerHandler serverHandler = new NettyHAServerHandler(this);
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childHandler(new ChannelInitializer<io.netty.channel.socket.SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) throws Exception {
                        ChannelPipeline pipeline = channel.pipeline();
                        pipeline.addLast("decoder", new NettyHADecoder());
                        pipeline.addLast("encoder", new NettyHAEncoder());
                        pipeline.addLast("serverHandler", serverHandler);
                    }
                });
            ChannelFuture future = bootstrap.bind(port).sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    private void destroyConnections() {

    }

    @Override
    public boolean changeToMaster(int masterEpoch) {
        final long lastEpoch = this.epochCache.getLastEpoch();
        if (masterEpoch < lastEpoch) {
            return false;
        }
        destroyConnections();

        // Stop ha client if needed
        if (this.haClient != null) {
            this.haClient.shutdown();
        }

        long maxPhyOffset = this.defaultMessageStore.getMaxPhyOffset();
        long minPhyOffset = this.defaultMessageStore.getMinPhyOffset();

        // Truncate dirty file
        final long truncateSize = truncateStrategy.truncateInvalidMsg(defaultMessageStore, maxPhyOffset);
        LOGGER.info("Truncate msg file size:{}, minPhyOffset:{}, maxPhyOffset:{}",
            truncateSize, minPhyOffset, maxPhyOffset);

        if (minPhyOffset >= 0) {
            this.epochCache.truncateSuffixByOffset(minPhyOffset);
        }

        if (maxPhyOffset >= 0) {
            this.epochCache.truncateSuffixByEpoch(masterEpoch);
            this.epochCache.tryAppendEpochEntry(new EpochEntry(masterEpoch, maxPhyOffset));
        }

        // Rollback cq index in the end
        this.defaultMessageStore.recoverTopicQueueTable();

        LOGGER.info("Change ha to master success, newMasterEpoch:{}, startOffset:{}", masterEpoch, maxPhyOffset);
        return true;
    }

    @Override
    public boolean changeToSlave(String newMasterAddr, int newMasterEpoch, Long slaveId) {
        final long lastEpoch = this.epochCache.getLastEpoch();
        if (newMasterEpoch <= lastEpoch) {
            LOGGER.error("change to slave failed, new master epoch is too small, {} {}",
                newMasterEpoch, lastEpoch);
            return false;
        }

        try {
            destroyConnections();
            if (this.haClient == null) {
                this.haClient = new AutoSwitchHAClient(defaultMessageStore, this.epochCache);
            } else {
                this.haClient.reOpen();
            }
            this.setLocalAddress(this.localAddress);
            this.haClient.updateSlaveId(slaveId);
            this.haClient.updateMasterAddress(newMasterAddr);
            this.haClient.updateHaMasterAddress(null);
            this.haClient.start();
            LOGGER.info("Change ha to slave success, newMasterAddress:{}, newMasterEpoch:{}",
                newMasterAddr, newMasterEpoch);
            return true;
        } catch (Exception e) {
            LOGGER.error("Error happen when change ha to slave", e);
            return false;
        }
    }

    @Override
    public HAClient getHAClient() {
        return this.haClient;
    }

    @Override
    public AtomicLong getPush2SlaveMaxOffset() {
        return null;
    }

    @Override
    public HARuntimeInfo getRuntimeInfo(long masterPutWhere) {
        return null;
    }

    @Override
    public WaitNotifyObject getWaitNotifyObject() {
        return null;
    }

    @Override
    public boolean isSlaveOK(long masterPutWhere) {
        return false;
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateHaMasterAddress(newAddr);
        }
    }

    @Override
    public int inSyncSlaveNums(long masterPutWhere) {
        return 0;
    }

    @Override
    public AtomicInteger getConnectionCount() {
        return null;
    }

    @Override
    public void putRequest(CommitLog.GroupCommitRequest request) {

    }

    @Override
    public void putGroupConnectionStateRequest(HAConnectionStateNotificationRequest request) {

    }

    @Override
    public List<HAConnection> getConnectionList() {
        return null;
    }

    @Override
    public void updateMasterAddress(String newAddr) {
    }

    public void removeConnection(HAConnection conn) {
        final Set<String> syncStateSet = getSyncStateSet();
        String slave = ((AutoSwitchHAConnection) conn).getSlaveAddress();
        if (syncStateSet.contains(slave)) {
            syncStateSet.remove(slave);
            notifySyncStateSetChanged(syncStateSet);
        }
        //super.removeConnection(conn);
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

    /**
     * Check and maybe shrink the inSyncStateSet.
     * A slave will be removed from inSyncStateSet if (curTime - HaConnection.lastCaughtUpTime) > option(haMaxTimeSlaveNotCatchup)
     */
    public Set<String> maybeShrinkInSyncStateSet() {
        final Set<String> currentSyncStateSet = getSyncStateSet();
        final HashSet<String> newSyncStateSet = new HashSet<>(currentSyncStateSet);
        final long haMaxTimeSlaveNotCatchup =
            this.defaultMessageStore.getMessageStoreConfig().getHaMaxTimeSlaveNotCatchup();
        for (HAConnection haConnection : this.connectionList) {
            final AutoSwitchHAConnection connection = (AutoSwitchHAConnection) haConnection;
            final String slaveAddress = connection.getSlaveAddress();
            if (currentSyncStateSet.contains(slaveAddress)) {
                if (connection.getSlaveAckOffset() < 0 ||
                    this.defaultMessageStore.getMaxPhyOffset() == connection.getSlaveAckOffset()) {
                    continue;
                }
                if ((System.currentTimeMillis() - connection.getLastCatchUpTimeMs()) > haMaxTimeSlaveNotCatchup) {
                    newSyncStateSet.remove(slaveAddress);
                }
            }
        }
        return newSyncStateSet;
    }

    /**
     * Check and maybe add the slave to inSyncStateSet.
     * A slave will be added to inSyncStateSet if its slaveMaxOffset >= current confirmOffset, and it is caught up to
     * an offset within the current leader epoch.
     */
    public void maybeExpandInSyncStateSet(final String slaveAddress, final long slaveMaxOffset) {
        final Set<String> currentSyncStateSet = getSyncStateSet();
        if (currentSyncStateSet.contains(slaveAddress)) {
            return;
        }
        final long confirmOffset = getConfirmOffset();
        if (slaveMaxOffset >= confirmOffset) {
            final EpochEntry currentLeaderEpoch = this.epochCache.getLastEntry();
            if (slaveMaxOffset >= currentLeaderEpoch.getStartOffset()) {
                currentSyncStateSet.add(slaveAddress);
                // Notify the upper layer that syncStateSet changed.
                notifySyncStateSetChanged(currentSyncStateSet);
            }
        }
    }

    /**
     * Get confirm offset (min slaveAckOffset of all syncStateSet members)
     */
    public long getConfirmOffset() {
        final Set<String> currentSyncStateSet = getSyncStateSet();
        long confirmOffset = this.defaultMessageStore.getMaxPhyOffset();
        for (HAConnection connection : this.connectionList) {
            if (currentSyncStateSet.contains(connection.getClientAddress())) {
                confirmOffset = Math.min(confirmOffset, connection.getSlaveAckOffset());
            }
        }
        return confirmOffset;
    }

    public synchronized void setSyncStateSet(final Set<String> syncStateSet) {
        this.syncStateSet.clear();
        this.syncStateSet.addAll(syncStateSet);
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

    public void setLocalAddress(String localAddress) {
        this.localAddress = localAddress;
    }

    public List<EpochEntry> getEpochEntries() {
        return this.epochCache.getAllEntries();
    }
}

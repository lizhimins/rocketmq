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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.ha.FlowMonitor;
import org.apache.rocketmq.store.ha.HAClient;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.apache.rocketmq.store.ha.netty.HAMessage;
import org.apache.rocketmq.store.ha.netty.HAMessageType;
import org.apache.rocketmq.store.ha.netty.NettyHAClientHandler;
import org.apache.rocketmq.store.ha.netty.NettyHADecoder;
import org.apache.rocketmq.store.ha.netty.NettyHAEncoder;
import org.apache.rocketmq.store.ha.protocol.ConfirmTruncate;
import org.apache.rocketmq.store.ha.protocol.HandshakeSlave;
import org.apache.rocketmq.store.ha.protocol.PushCommitLogAck;

public class AutoSwitchHAClient extends ServiceThread implements HAClient {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final int READ_MAX_BUFFER_SIZE = 4 * 1024 * 1024;
    private static final int WRITE_MAX_BUFFER_SIZE = 16 * 1024 * 1024;

    private final AtomicReference<String> masterHaAddress = new AtomicReference<>();
    private final AtomicReference<String> masterAddress = new AtomicReference<>();
    private final AtomicReference<Long> slaveId = new AtomicReference<>();

    private final TruncateStrategy truncateStrategy = new MoveAndDiscardTruncateStrategy();
    private final DefaultMessageStore messageStore;

    private long masterEpoch;
    private final EpochStore epochCache;
    private FlowMonitor flowMonitor;

    /**
     * Confirm offset = min(localMaxOffset, master confirm offset).
     */
    private long confirmOffset;
    private long currentReceivedEpoch;
    private long currentReportedOffset;
    private volatile HAConnectionState currentState;

    public EventLoopGroup workerGroup;
    public Bootstrap bootstrap;
    private ChannelFuture future;

    private final NettyHAEncoder nettyHAEncoder = new NettyHAEncoder();
    private final NettyHADecoder nettyHADecoder = new NettyHADecoder();
    private final NettyHAClientHandler clientHandler = new NettyHAClientHandler();
    private Channel channel;

    public AutoSwitchHAClient(DefaultMessageStore defaultMessageStore, EpochStore epochCache) throws IOException {
        this.messageStore = defaultMessageStore;
        this.epochCache = epochCache;
        startNettyClient();
        init();
    }

    public void init() throws IOException {
        this.flowMonitor = new FlowMonitor(this.messageStore.getMessageStoreConfig());
        changeCurrentState(HAConnectionState.READY);
        this.currentReceivedEpoch = -1;
        this.currentReportedOffset = 0;
        this.confirmOffset = -1;
    }

    public void reOpen() throws IOException {
        shutdown();
        init();
    }

    @Override
    public String getServiceName() {
        if (messageStore.getBrokerConfig().isInBrokerContainer()) {
            return messageStore.getBrokerIdentity().getLoggerIdentifier()
                + AutoSwitchHAClient.class.getSimpleName();
        }
        return AutoSwitchHAClient.class.getSimpleName();
    }

    public void updateSlaveId(Long newId) {
        Long currentId = this.slaveId.get();
        if (this.slaveId.compareAndSet(currentId, newId)) {
            LOGGER.info("Update slave Id, OLD: {}, New: {}", currentId, newId);
        }
    }

    @Override
    public void updateMasterAddress(String newAddress) {
        String currentAddr = this.masterAddress.get();
        if (!StringUtils.equals(newAddress, currentAddr) && masterAddress.compareAndSet(currentAddr, newAddress)) {
            LOGGER.info("update master address, OLD: " + currentAddr + " NEW: " + newAddress);
        }
    }

    @Override
    public void updateHaMasterAddress(String newAddress) {
        String currentAddr = this.masterHaAddress.get();
        if (!StringUtils.equals(newAddress, currentAddr) && masterHaAddress.compareAndSet(currentAddr, newAddress)) {
            LOGGER.info("update master ha address, OLD: " + currentAddr + " NEW: " + newAddress);
            wakeup();
        }
    }

    @Override
    public String getMasterAddress() {
        return this.masterAddress.get();
    }

    @Override
    public String getHaMasterAddress() {
        return this.masterHaAddress.get();
    }

    @Override
    public long getLastReadTimestamp() {
        return this.nettyHADecoder.getLastReadTimestamp();
    }

    @Override
    public long getLastWriteTimestamp() {
        return this.nettyHAEncoder.getLastWriteTimestamp();
    }

    @Override
    public HAConnectionState getCurrentState() {
        return this.currentState;
    }

    @Override
    public void changeCurrentState(HAConnectionState haConnectionState) {
        LOGGER.info("change state to {}", haConnectionState);
        this.currentState = haConnectionState;
    }

    public void closeMasterAndWait() {
        this.closeMaster();
        this.waitForRunning(1000 * 5);
    }

    @Override
    public void closeMaster() {
        // close channel
        LOGGER.info("AutoSwitchHAClient close connection with master {}", this.masterHaAddress.get());
        this.changeCurrentState(HAConnectionState.READY);
    }

    @Override
    public long getTransferredByteInSecond() {
        return this.flowMonitor.getTransferredByteInSecond();
    }

    @Override
    public void shutdown() {
        changeCurrentState(HAConnectionState.SHUTDOWN);
        // Shutdown thread firstly
        this.flowMonitor.shutdown();
        super.shutdown();

        closeMaster();
    }

    private boolean isTimeToReportOffset() {
        long interval = this.messageStore.now() - this.nettyHAEncoder.getLastWriteTimestamp();
        return interval > this.messageStore.getMessageStoreConfig().getHaSendHeartbeatInterval();
    }

    private boolean sendHandshakeSlave() throws IOException {
        BrokerConfig brokerConfig = messageStore.getBrokerConfig();
        HandshakeSlave handshakeSlave = new HandshakeSlave();
        handshakeSlave.setClusterName(brokerConfig.getBrokerClusterName());
        handshakeSlave.setBrokerName(brokerConfig.getBrokerName());
        handshakeSlave.setBrokerId(brokerConfig.getBrokerId());
        handshakeSlave.setBrokerAppVersion(MQVersion.CURRENT_VERSION);
        handshakeSlave.setLanguageCode(LanguageCode.JAVA);
        handshakeSlave.setHaProtocolVersion(2);

        HAMessage haMessage = new HAMessage(HAMessageType.SLAVE_HANDSHAKE, RemotingSerializable.encode(handshakeSlave));
        channel.writeAndFlush(haMessage);
        return true;
    }

    private void handshakeWithMaster() throws IOException {
        boolean result = this.sendHandshakeSlave();
        if (!result) {
            closeMasterAndWait();
        }
    }

    private void reportSlaveMaxOffset() {
        final long maxPhyOffset = this.messageStore.getMaxPhyOffset();
        if (maxPhyOffset > this.currentReportedOffset) {
            this.currentReportedOffset = maxPhyOffset;
            this.sendPushCommitLogAck(this.currentReportedOffset);
        }
    }

    public void startNettyClient() {
        workerGroup = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast("decoder", nettyHADecoder);
                    ch.pipeline().addLast("encoder", nettyHAEncoder);
                    ch.pipeline().addLast(clientHandler);
                }
            });
    }

    public void doNettyConnect(SocketAddress socketAddress) throws InterruptedException {
        if (future != null && future.channel() != null && future.channel().isActive()) {
            return;
        }

        future = bootstrap.connect(socketAddress);
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    System.out.println("connect to server successfully!");
                    LOGGER.info("connect to server successfully!");
                } else {
                    System.out.println("Failed to connect to server, try connect after 1000 ms");
                    LOGGER.info("Failed to connect to server, try connect after 1000 ms");
                    future.channel().eventLoop().schedule(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                doNettyConnect(socketAddress);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }, 1000, TimeUnit.MILLISECONDS);
                }
            }
        }).sync();
    }

    public boolean tryConnectToMaster() throws IOException, InterruptedException {
        if (null == this.channel) {
            String addr = this.masterHaAddress.get();
            if (StringUtils.isNotEmpty(addr)) {
                SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                doNettyConnect(socketAddress);
                //changeCurrentState(HAConnectionState.HANDSHAKE);
                sendHandshakeSlave();
            }
            this.currentReportedOffset = this.messageStore.getMaxPhyOffset();
        }

        return this.sendHandshakeSlave();
    }

    private boolean transferFromMaster() throws IOException {
        if (isTimeToReportOffset()) {
            LOGGER.info("Slave report current offset {}", this.currentReportedOffset);
            this.sendPushCommitLogAck(this.currentReportedOffset);
        }
        return true;
    }

    private List<EpochEntry> queryMasterEpoch() {
        HAMessage haMessage = new HAMessage(HAMessageType.QUERY_EPOCH);
        haMessage.setEpoch(epochCache.getLastEpoch());
        channel.writeAndFlush(haMessage);
        return new ArrayList<>();
    }

    private void sendConfirmTruncateToMaster(long startOffset) {
        ConfirmTruncate confirmTruncate = new ConfirmTruncate(startOffset);
        HAMessage haMessage = new HAMessage(HAMessageType.CONFIRM_TRUNCATE);
        haMessage.setEpoch(epochCache.getLastEpoch());
        haMessage.setBody(Objects.requireNonNull(RemotingSerializable.encode(confirmTruncate)));
        channel.writeAndFlush(haMessage);
        changeCurrentState(HAConnectionState.TRANSFER);
    }

    private void sendPushCommitLogAck(final long offsetToReport) {
        PushCommitLogAck pushCommitLogAck = new PushCommitLogAck();
        pushCommitLogAck.setConfirmOffset(offsetToReport);
        pushCommitLogAck.setReadOnly(messageStore.getBrokerConfig().isSlaveReadOnlyEnable());
        HAMessage haMessage = new HAMessage(HAMessageType.PUSH_ACK);
        haMessage.setEpoch(epochCache.getLastEpoch());
        haMessage.setBody(Objects.requireNonNull(RemotingSerializable.encode(pushCommitLogAck)));
        channel.writeAndFlush(haMessage);
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service started");

        this.flowMonitor.start();
        while (!this.isStopped()) {
            try {
                switch (this.currentState) {
                    case READY:
                        tryConnectToMaster();
                        continue;
                    case HANDSHAKE:
                        List<EpochEntry> epochEntryList = queryMasterEpoch();
                        doTruncateFiles(epochEntryList);
                        sendConfirmTruncateToMaster(this.currentReportedOffset);
                        continue;
                    case TRANSFER:
                        if (!transferFromMaster()) {
                            closeMasterAndWait();
                            continue;
                        }
                        break;
                    case SHUTDOWN:
                        return;
                    case SUSPEND:
                    default:
                        waitForRunning(1000);
                        continue;
                }
                long interval = this.messageStore.now() - this.nettyHADecoder.getLastReadTimestamp();
                if (interval > this.messageStore.getMessageStoreConfig().getHaHousekeepingInterval()) {
                    LOGGER.warn("NettyHAClient housekeeping, found this connection {} expired, interval={}",
                        this.masterHaAddress, interval);
                    closeMaster();
                    LOGGER.warn("NettyHAClient, master not response some time, so close connection");
                }
            } catch (Exception e) {
                LOGGER.warn(this.getServiceName() + " service has exception. ", e);
                closeMasterAndWait();
            }
        }
    }

    /**
     * Compare the master and slave's epoch file, find consistent point, do truncate.
     */
    private boolean doTruncateFiles(List<EpochEntry> masterEpochEntries) throws IOException {

        if (this.epochCache.getAllEntries().size() == 0) {
            // If epochMap is empty, means the broker is a new replicas
            LOGGER.info("Slave local epochCache is empty, skip truncate log");
            this.currentReportedOffset = 0;
            return true;
        }

        // TODO: set max phy commitLog offset
        final EpochStore masterEpochCache = new EpochFileStore();
        masterEpochCache.initStateFromEntries(masterEpochEntries);

        final EpochStore localEpochCache = new EpochFileStore();
        final List<EpochEntry> localEpochEntries = this.epochCache.getAllEntries();
        localEpochCache.initStateFromEntries(localEpochEntries);

        final long truncateOffset = localEpochCache.findLastConsistentPoint(masterEpochCache);

        if (truncateOffset < 0) {
            // If truncateOffset < 0, means we can't find a consistent point
            LOGGER.error("Failed to find a consistent point between masterEpoch:{} and slaveEpoch:{}",
                masterEpochEntries, localEpochEntries);
            return false;
        }

        boolean syncFromLastFile = this.messageStore.getMessageStoreConfig().isSyncFromLastFile();

        // Truncate invalid msg first
        if (0 > truncateStrategy.truncateInvalidMsg(messageStore, truncateOffset)) {
            LOGGER.error("Failed to truncate slave log to {}", truncateOffset);
            return false;
        }

        // Truncate epoch
        this.epochCache.truncateSuffixByOffset(truncateOffset);
        LOGGER.info("Truncate slave log to {} success, change to transfer state", truncateOffset);

        changeCurrentState(HAConnectionState.TRANSFER);
        this.currentReportedOffset = truncateOffset;
        return true;
    }

    private void doPutCommitLog(long currentReceivedEpoch, long masterOffset, ByteBuf commitLogByteBuf,
        int dataStart, int dataLength) {

        long slavePhyOffset = this.messageStore.getMaxPhyOffset();
        if (slavePhyOffset != 0) {
            //if (slavePhyOffset != masterOffset) {
            //    LOGGER.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
            //        + slavePhyOffset + " MASTER: " + masterOffset);
            //    return false;
            //}
        }

        // If epoch changed
        if (masterEpoch != this.currentReceivedEpoch) {
            this.epochCache.tryAppendEpochEntry(new EpochEntry(masterEpoch, masterOffset));
        }

        this.confirmOffset = Math.min(confirmOffset, messageStore.getMaxPhyOffset());

        if (commitLogByteBuf.readableBytes() > 0) {
            this.messageStore.appendToCommitLog(masterOffset, commitLogByteBuf.array(), dataStart, dataLength);
        }

        reportSlaveMaxOffset();
    }
}

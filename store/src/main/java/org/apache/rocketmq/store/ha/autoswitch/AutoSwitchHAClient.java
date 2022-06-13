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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
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
import org.apache.rocketmq.store.ha.protocol.HandshakeMaster;
import org.apache.rocketmq.store.ha.protocol.HandshakeResult;
import org.apache.rocketmq.store.ha.protocol.HandshakeSlave;
import org.apache.rocketmq.store.ha.protocol.PushCommitLogAck;

public class AutoSwitchHAClient extends ServiceThread implements HAClient {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final int READ_MAX_BUFFER_SIZE = 16 * 1024 * 1024;
    private static final int WRITE_MAX_BUFFER_SIZE = 4 * 1024 * 1024;

    private final AtomicReference<String> masterHaAddress = new AtomicReference<>();
    private final AtomicReference<String> masterAddress = new AtomicReference<>();
    private final AtomicReference<Long> slaveId = new AtomicReference<>();

    private final TruncateStrategy truncateStrategy = new MoveAndDiscardTruncateStrategy();
    private final DefaultMessageStore messageStore;

    private final EpochStore epochCache;
    private FlowMonitor flowMonitor;

    /**
     * Confirm offset = min(localMaxOffset, master confirm offset).
     */
    private long confirmOffset;
    private long currentMasterEpoch;
    private long currentReceivedEpoch;
    private long currentReportedOffset;
    private volatile HAConnectionState currentState;

    public EventLoopGroup workerGroup;
    public Bootstrap bootstrap;
    private ChannelFuture future;
    private ChannelPromise channelPromise;

    private final NettyHAEncoder nettyHAEncoder = new NettyHAEncoder();
    private final NettyHADecoder nettyHADecoder = new NettyHADecoder();
    private final NettyHAClientHandler clientHandler = new NettyHAClientHandler(this);

    public AutoSwitchHAClient(DefaultMessageStore defaultMessageStore, EpochStore epochCache) throws IOException {
        this.messageStore = defaultMessageStore;
        this.epochCache = epochCache;
        startNettyClient();
        init();
    }

    public void init() throws IOException {
        if (this.flowMonitor == null) {
            this.flowMonitor = new FlowMonitor(this.messageStore.getMessageStoreConfig());
        }
        this.currentReceivedEpoch = -1L;
        this.currentReportedOffset = 0L;
        this.confirmOffset = -1L;
        changeCurrentState(HAConnectionState.READY);
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

    public long getCurrentMasterEpoch() {
        return currentMasterEpoch;
    }

    public void setCurrentMasterEpoch(long currentMasterEpoch) {
        this.currentMasterEpoch = currentMasterEpoch;
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
        this.future.channel().close();
        LOGGER.info("AutoSwitchHAClient close connection with master {}", this.masterHaAddress.get());
    }

    @Override
    public long getTransferredByteInSecond() {
        return this.flowMonitor.getTransferredByteInSecond();
    }

    @Override
    public void shutdown() {
        changeCurrentState(HAConnectionState.SHUTDOWN);
        this.flowMonitor.shutdown();
        super.shutdown();
        closeMaster();
    }

    private boolean isTimeToReportOffset() {
        long interval = this.messageStore.now() - this.nettyHAEncoder.getLastWriteTimestamp();
        return interval > this.messageStore.getMessageStoreConfig().getHaSendHeartbeatInterval();
    }

    private void sendHandshakeSlave(Channel channel) {
        BrokerConfig brokerConfig = messageStore.getBrokerConfig();
        HandshakeSlave handshakeSlave = new HandshakeSlave();
        handshakeSlave.setClusterName(brokerConfig.getBrokerClusterName());
        handshakeSlave.setBrokerName(brokerConfig.getBrokerName());
        handshakeSlave.setBrokerId(brokerConfig.getBrokerId());
        handshakeSlave.setBrokerAddr(((AutoSwitchHAService) messageStore.getHaService()).getLocalAddress());
        handshakeSlave.setBrokerAppVersion(MQVersion.CURRENT_VERSION);
        handshakeSlave.setLanguageCode(LanguageCode.JAVA);
        handshakeSlave.setHaProtocolVersion(2);

        HAMessage haMessage = new HAMessage(HAMessageType.SLAVE_HANDSHAKE, currentMasterEpoch,
            RemotingSerializable.encode(handshakeSlave));
        channel.writeAndFlush(haMessage);
    }

    public synchronized void reportSlaveMaxOffset() {
        final long maxPhyOffset = this.messageStore.getMaxPhyOffset();
        if (this.currentReportedOffset < maxPhyOffset) {
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
            .option(ChannelOption.SO_SNDBUF, WRITE_MAX_BUFFER_SIZE)
            .option(ChannelOption.SO_RCVBUF, READ_MAX_BUFFER_SIZE)
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
        future.addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                System.out.println("HAClient connect to server successfully! " + socketAddress.toString());
                LOGGER.info("HAClient connect to server successfully!");
            } else {
                System.out.println("Failed to connect to server, try connect after 1000 ms" + socketAddress.toString());
                LOGGER.info("Failed to connect to server, try connect after 1000 ms");
                future.channel().eventLoop().schedule(() -> {
                    try {
                        doNettyConnect(socketAddress);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }, 1000, TimeUnit.MILLISECONDS);
            }
        });

        try {
            future.sync();
        } catch (InterruptedException e) {
            throw new RuntimeException("HAClient start server InterruptedException", e);
        }
    }

    public boolean tryConnectToMaster() throws InterruptedException {
        try {
            String addr = this.masterHaAddress.get();
            if (StringUtils.isNotEmpty(addr)) {
                SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                doNettyConnect(socketAddress);
                changeCurrentState(HAConnectionState.READY);
                Channel channel = future.channel();
                channelPromise = new DefaultChannelPromise(channel);
                sendHandshakeSlave(channel);
                channelPromise.await(5000);
                if (channelPromise.isSuccess()) {
                    changeCurrentState(HAConnectionState.HANDSHAKE);
                    //System.out.println("client change to handshake");
                }
                return channelPromise.isSuccess();
            }
        } catch (InterruptedException e) {
            System.out.println("HAClient send handshake but not receive response" + e);
            LOGGER.error("HAClient send handshake but not receive response, masterAddr:{}", masterHaAddress.get(), e);
            future.channel().close().sync();
        }
        return false;
    }

    private boolean transferFromMaster() throws IOException {
        //if (isTimeToReportOffset()) {
            LOGGER.info("timer report slave offset: {}", this.currentReportedOffset);
            this.sendPushCommitLogAck(this.currentReportedOffset);
        //}
        return true;
    }

    public void masterHandshake(HandshakeMaster handshakeMaster) {
        if (handshakeMaster != null
            && HandshakeResult.ACCEPT.equals(handshakeMaster.getHandshakeResult())) {
            channelPromise.setSuccess();
        }
        LOGGER.error("Master reject build connection, {}", handshakeMaster);
        channelPromise.setFailure(new Exception("Master reject build connection"));
    }

    private boolean queryMasterEpoch() throws InterruptedException {
        try {
            HAMessage haMessage = new HAMessage(HAMessageType.QUERY_EPOCH, currentMasterEpoch);
            channelPromise = new DefaultChannelPromise(future.channel());
            future.channel().writeAndFlush(haMessage);
            channelPromise.await(5000);
            if (channelPromise.isSuccess()) {
                //System.out.println("client change to transfer");
                changeCurrentState(HAConnectionState.TRANSFER);
            }
            return channelPromise.isSuccess();
        } catch (InterruptedException e) {
            System.out.println("query epoch failed");
            future.channel().close().sync();
        }
        return false;
    }

    public void doConsistencyRepairWithMaster(List<EpochEntry> entryList) {
        if (!doTruncateFiles(entryList)) {
            return;
        }
        sendConfirmTruncateToMaster(messageStore.getMaxPhyOffset());
        changeCurrentState(HAConnectionState.TRANSFER);
        channelPromise.setSuccess();
    }

    private void sendConfirmTruncateToMaster(long startOffset) {
        ConfirmTruncate confirmTruncate = new ConfirmTruncate(startOffset);
        HAMessage haMessage = new HAMessage(HAMessageType.CONFIRM_TRUNCATE, currentMasterEpoch,
            RemotingSerializable.encode(confirmTruncate));
        future.channel().writeAndFlush(haMessage);
    }

    public synchronized void sendPushCommitLogAck(final long offsetToReport) {
        PushCommitLogAck pushCommitLogAck = new PushCommitLogAck();
        pushCommitLogAck.setConfirmOffset(offsetToReport);
        pushCommitLogAck.setReadOnly(messageStore.getMessageStoreConfig().isAsyncLearner());
        HAMessage haMessage = new HAMessage(HAMessageType.PUSH_ACK, currentMasterEpoch,
            RemotingSerializable.encode(pushCommitLogAck));
        future.channel().writeAndFlush(haMessage);
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service started");

        this.flowMonitor.start();
        while (!this.isStopped()) {
            try {
                switch (this.currentState) {
                    case READY:
                        if (!tryConnectToMaster()) {
                            this.waitForRunning(1000);
                        }
                        continue;
                    case HANDSHAKE:
                        if (!queryMasterEpoch()) {
                            this.waitForRunning(1000);
                        }
                        continue;
                    case TRANSFER:
                        // only do flow control and monitor
                        if (!transferFromMaster()) {
                            closeMasterAndWait();
                            break;
                        }
                        this.waitForRunning(1000);
                        continue;
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
    private boolean doTruncateFiles(List<EpochEntry> masterEpochEntries) {

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

    public void doPutCommitLog(long currentBlockEpoch, long masterOffset, ByteBuffer byteBuffer) {
        long slavePhyOffset = this.messageStore.getMaxPhyOffset();
        if (slavePhyOffset != masterOffset) {
            System.out.printf("Put commitLog error, slave: %d, master offset: %d%n", slavePhyOffset, masterOffset);
            return;
        }

        // If epoch changed to bigger, last epoch record would be terminated
        if (this.currentReceivedEpoch < currentBlockEpoch) {
            this.currentReceivedEpoch = currentBlockEpoch;
            this.epochCache.tryAppendEpochEntry(new EpochEntry(currentMasterEpoch, masterOffset));
        }

        if (byteBuffer.hasRemaining()) {
            this.messageStore.appendToCommitLog(
                masterOffset, byteBuffer.array(), 16, byteBuffer.remaining());
        }
        this.confirmOffset = Math.min(confirmOffset, messageStore.getMaxPhyOffset());
        this.reportSlaveMaxOffset();
    }
}

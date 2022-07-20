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
import org.apache.rocketmq.store.ha.file.MoveAndDiscardTruncateStrategy;
import org.apache.rocketmq.store.ha.file.TruncateStrategy;
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
import org.apache.rocketmq.store.ha.protocol.PushCommitLogData;

public class AutoSwitchHAClient extends ServiceThread implements HAClient {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final int READ_MAX_BUFFER_SIZE = 16 * 1024 * 1024;
    private static final int WRITE_MAX_BUFFER_SIZE = 4 * 1024 * 1024;

    private final AtomicReference<String> masterHaAddress = new AtomicReference<>();
    private final AtomicReference<String> masterAddress = new AtomicReference<>();
    private final AtomicReference<Long> slaveId = new AtomicReference<>();

    private final AutoSwitchHAService haService;
    private final EpochStore epochCache;
    private final DefaultMessageStore messageStore;
    private final TruncateStrategy truncateStrategy;
    private FlowMonitor flowMonitor;

    private volatile HAConnectionState currentState = HAConnectionState.SHUTDOWN;
    private volatile long currentMasterEpoch = -1L;
    private volatile long currentReceivedEpoch = -1L;
    private volatile long currentTransferOffset = -1L;
    private volatile long lastReadTimestamp;
    private volatile long lastWriteTimestamp;

    public EventLoopGroup workerGroup;
    public Bootstrap bootstrap;
    private ChannelFuture future;
    private ChannelPromise channelPromise;

    public AutoSwitchHAClient(AutoSwitchHAService haService) {
        this.haService = haService;
        this.messageStore = haService.getDefaultMessageStore();
        this.epochCache = haService.getEpochCache();
        this.truncateStrategy = new MoveAndDiscardTruncateStrategy();
    }

    public void init() throws IOException {
        if (this.flowMonitor == null) {
            this.flowMonitor = new FlowMonitor(this.messageStore.getMessageStoreConfig());
        }

        // init offset
        this.currentMasterEpoch = -1L;
        this.currentReceivedEpoch = -1L;
        this.currentTransferOffset = -1L;

        startNettyClient();
        changeCurrentState(HAConnectionState.READY);
    }

    public ChannelPromise getChannelPromise() {
        return channelPromise;
    }

    public void changePromise(boolean success) {
        if (this.channelPromise != null && !this.channelPromise.isDone()) {
            if (success) {
                this.channelPromise.setSuccess();
            } else {
                this.channelPromise.setFailure(new RuntimeException("promise failure"));
            }
        }
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
        this.slaveId.set(newId);
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
        return this.lastReadTimestamp;
    }

    @Override
    public long getLastWriteTimestamp() {
        return this.lastWriteTimestamp;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return this.currentState;
    }

    @Override
    public void changeCurrentState(HAConnectionState haConnectionState) {
        System.out.println("client change state: " + this.currentState + " => " + haConnectionState);
        LOGGER.info("change state to {}", haConnectionState);
        this.currentState = haConnectionState;
    }

    @Override
    public void closeMaster() {
        if (channelPromise != null) {
            channelPromise.setFailure(new RuntimeException("epoch not match"));
        }
        // close channel
        if (future != null && future.channel() != null) {
            try {
                System.out.println("channel close by client");
                future.channel().close().sync();
                future = null;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        LOGGER.info("AutoSwitchHAClient close connection with master {}", this.masterHaAddress.get());
    }

    @Override
    public long getTransferredByteInSecond() {
        return this.flowMonitor.getTransferredByteInSecond();
    }

    @Override
    public void shutdown() {
        changeCurrentState(HAConnectionState.SHUTDOWN);
        closeMaster();
        this.flowMonitor.shutdown();
        super.shutdown();
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

        //System.out.println("hand shake: " + handshakeSlave);
        HAMessage haMessage = new HAMessage(HAMessageType.SLAVE_HANDSHAKE, currentMasterEpoch,
            RemotingSerializable.encode(handshakeSlave));

        this.lastWriteTimestamp = System.currentTimeMillis();
        channel.writeAndFlush(haMessage);
    }

    public synchronized void startNettyClient() {
        AutoSwitchHAClient haClient = this;
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
                    ch.pipeline().addLast("decoder", new NettyHADecoder());
                    ch.pipeline().addLast("encoder", new NettyHAEncoder());
                    ch.pipeline().addLast(new NettyHAClientHandler(haClient));
                }
            });
    }

    public void doNettyConnect() throws InterruptedException {
        if (future != null && future.channel() != null && future.channel().isActive()) {
            return;
        }

        if (StringUtils.isBlank(this.masterHaAddress.get())) {
            return;
        }

        SocketAddress socketAddress = RemotingUtil.string2SocketAddress(this.masterHaAddress.get());
        future = bootstrap.connect(socketAddress).addListener((ChannelFutureListener) future -> {
            if (future.isSuccess()) {
                System.out.println("client connect to server successfully! " +
                    this.messageStore.getBrokerConfig().getBrokerName() + " " + socketAddress + " " + future.channel().id());
                LOGGER.info("client connect to server successfully!");
            } else {
                System.out.println("remote: " + future.channel().remoteAddress() + ", local: " + future.channel().localAddress() + " " + future.cause().toString());
                System.out.println("Failed to connect to server, try connect after 1000 ms" + socketAddress);
                LOGGER.info("Failed to connect to server, try connect after 1000 ms");
                //future.channel().eventLoop().schedule(() -> {
                //    try {
                //        doNettyConnect();
                //    } catch (InterruptedException e) {
                //        e.printStackTrace();
                //        System.out.println(e);
                //    }
                //}, 1000, TimeUnit.MILLISECONDS);
            }
        });

        try {
            future.await();
        } catch (InterruptedException e) {
            throw new RuntimeException("HAClient start server InterruptedException", e);
        }
    }

    public synchronized boolean tryConnectToMaster() throws InterruptedException {
        try {
            String address = this.masterHaAddress.get();
            if (StringUtils.isNotEmpty(address)) {
                doNettyConnect();
                channelPromise = new DefaultChannelPromise(future.channel());
                sendHandshakeSlave(future.channel());
                boolean receiveResult = channelPromise.await(5000);
                if (channelPromise.isSuccess()) {
                    channelPromise = null;
                    changeCurrentState(HAConnectionState.HANDSHAKE);
                    return true;
                } else {
                    System.out.println("handshake failed");
                }
                channelPromise = null;
                if (!receiveResult) {
                    System.out.println("client not receive handshake signal");
                }
            }
        } catch (InterruptedException e) {
            System.out.println("HAClient send handshake but not receive response" + e);
            LOGGER.error("HAClient send handshake but not receive response, masterAddr:{}", masterHaAddress.get(), e);
            future.channel().close().sync();
        }
        return false;
    }

    private boolean isTimeToReportOffset() {
        long interval = this.messageStore.now() - this.lastWriteTimestamp;
        //return interval > this.messageStore.getMessageStoreConfig().getHaSendHeartbeatInterval();
        return interval > 10 * 1000L;
    }

    private boolean checkConnectionTimeout() {
//        if (isTimeToReportOffset()) {
//            System.out.println("detect connection timeout, last=" + this.lastWriteTimestamp + " " + System.currentTimeMillis());
//            LOGGER.info("schedule to report slave offset: {}", this.currentTransferOffset);
//            return false;
//        }
        return true;
    }

    public void masterHandshake(HandshakeMaster handshakeMaster) {
        if (handshakeMaster != null
            && HandshakeResult.ACCEPT.equals(handshakeMaster.getHandshakeResult())) {
            channelPromise.setSuccess();
            return;
        }
        LOGGER.error("Master reject build connection, {}", handshakeMaster);
        channelPromise.setFailure(new Exception("Master reject build connection"));
    }

    private boolean queryMasterEpoch() throws InterruptedException {
        try {
            HAMessage haMessage = new HAMessage(HAMessageType.QUERY_EPOCH, currentMasterEpoch);
            channelPromise = new DefaultChannelPromise(future.channel());
            this.lastWriteTimestamp = System.currentTimeMillis();
            future.channel().writeAndFlush(haMessage);
            channelPromise.await(5000);
            if (channelPromise.isSuccess()) {
                channelPromise = null;
                changeCurrentState(HAConnectionState.TRANSFER);
                return true;
            }
            channelPromise = null;
            return false;
        } catch (InterruptedException e) {
            System.out.println("query epoch failed");
            future.channel().close().sync();
        }
        return true;
    }

    public synchronized void doConsistencyRepairWithMaster(List<EpochEntry> entryList) {
        channelPromise.setSuccess();
        if (!doTruncateFiles(entryList)) {
            this.closeMaster();
            return;
        }

        long masterMinOffset = entryList.get(0).getStartOffset();
        long masterMaxOffset = entryList.get(entryList.size() - 1).getEndOffset();

        // only take effect when slave commitLog is empty
        if (currentTransferOffset == -1L) {
            boolean fromLast = this.messageStore.getMessageStoreConfig().isSyncFromLastFile();
            currentTransferOffset = fromLast ? masterMaxOffset : masterMinOffset;
        } else {
            currentTransferOffset = Math.max(currentTransferOffset, masterMinOffset);
            currentTransferOffset = Math.min(currentTransferOffset, masterMaxOffset);
        }

        sendConfirmTruncateToMaster(currentTransferOffset);
    }

    private void sendConfirmTruncateToMaster(long startOffset) {
        ConfirmTruncate confirmTruncate = new ConfirmTruncate(startOffset);
        HAMessage haMessage = new HAMessage(HAMessageType.CONFIRM_TRUNCATE, currentMasterEpoch,
            RemotingSerializable.encode(confirmTruncate));
        this.lastWriteTimestamp = System.currentTimeMillis();
        future.channel().writeAndFlush(haMessage);
    }

    public synchronized void sendPushCommitLogAck() {
        PushCommitLogAck pushCommitLogAck = new PushCommitLogAck();
        pushCommitLogAck.setConfirmOffset(this.currentTransferOffset);
        pushCommitLogAck.setReadOnly(messageStore.getMessageStoreConfig().isAsyncLearner());
        HAMessage haMessage = new HAMessage(HAMessageType.PUSH_ACK, currentMasterEpoch,
            RemotingSerializable.encode(pushCommitLogAck));
        this.lastWriteTimestamp = System.currentTimeMillis();
        future.channel().writeAndFlush(haMessage);
        System.out.printf("send ack, offset=%d, async role=%s%n",
            this.currentTransferOffset, messageStore.getMessageStoreConfig().isAsyncLearner());
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
                            closeMaster();
                            this.waitForRunning(3000);
                        }
                        continue;
                    case HANDSHAKE:
                        if (!queryMasterEpoch()) {
                            closeMaster();
                            this.waitForRunning(3000);
                        }
                        continue;
                    case TRANSFER:
                    case SUSPEND:
                        // only do flow control and housekeeping monitor
                        if (!checkConnectionTimeout()) {
                            closeMaster();
                            break;
                        }
                        this.waitForRunning(500);
                        continue;
                    case SHUTDOWN:
                    default:
                        waitForRunning(1000);
                        continue;
                }
                long interval = this.messageStore.now() - this.lastReadTimestamp;
                if (interval > this.messageStore.getMessageStoreConfig().getHaHousekeepingInterval()) {
                    LOGGER.warn("NettyHAClient housekeeping, found this connection {} expired, interval={}",
                        this.masterHaAddress, interval);
                    closeMaster();
                    LOGGER.warn("NettyHAClient, master not response some time, so close connection");
                }
            } catch (Exception e) {
                LOGGER.warn(this.getServiceName() + " service has exception. ", e);
                closeMaster();
            }
        }
    }

    /**
     * Compare the master and slave's epoch file, find consistent point, do truncate.
     */
    private boolean doTruncateFiles(List<EpochEntry> masterEpochEntries) {

        System.out.println("client receive master epoch: " + masterEpochEntries);

        // If epochMap is empty, means the broker is a new replicas
        if (this.epochCache.getAllEntries().size() == 0) {
            System.out.println("Slave local epochCache is empty, skip truncate log");
            LOGGER.info("Slave local epochCache is empty, skip truncate log");
            return true;
        }

        // TODO: set max phy commitLog offset
        final EpochStore masterEpochCache = new EpochStoreService();
        masterEpochCache.initStateFromEntries(masterEpochEntries);

        final EpochStore localEpochCache = new EpochStoreService();
        final List<EpochEntry> localEpochEntries = this.epochCache.getAllEntries();
        localEpochCache.initStateFromEntries(localEpochEntries);

        // If truncateOffset < 0, means we can't find a consistent point
        final long truncateOffset = localEpochCache.findLastConsistentPoint(masterEpochCache);
//        if (truncateOffset < 0) {
//            System.out.println("tr error");
//            LOGGER.error("Failed to find a consistent point between masterEpoch:{} and slaveEpoch:{}",
//                masterEpochEntries, localEpochEntries);
//            return false;
//        }

        // Truncate invalid msg first
        if (0 > truncateStrategy.truncateInvalidMsg(messageStore, truncateOffset)) {
            LOGGER.error("Failed to truncate slave log to {}", truncateOffset);
            return false;
        }

        // Truncate epoch
        this.epochCache.truncateSuffixByOffset(truncateOffset);
        LOGGER.info("Truncate slave log to {} success, change to transfer state", truncateOffset);

        this.currentTransferOffset = truncateOffset;
        return true;
    }

    public void doPutCommitLog(PushCommitLogData pushCommitLogData, ByteBuffer byteBuffer) {
        long currentBlockEpoch = pushCommitLogData.getEpoch();
        long confirmOffset = pushCommitLogData.getConfirmOffset();
        long masterOffset = pushCommitLogData.getStartOffset();
        long epochStartOffset = pushCommitLogData.getEpochStartOffset();
        long slavePhyOffset = this.messageStore.getMaxPhyOffset();

//        if (slavePhyOffset != 0) {
//            if (slavePhyOffset != masterOffset) {
//                LOGGER.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
//                    + slavePhyOffset + " MASTER: " + masterOffset);
//                throw new RuntimeException("offset not match");
//            }
//        }

        // Must put data first
        if (byteBuffer.hasRemaining()) {
            this.messageStore.appendToCommitLog(
                masterOffset, byteBuffer.array(), 32, byteBuffer.remaining());
        }

        System.out.println("client print confirm offset before update: " + haService.getLocalAddress() + " " + haService.getConfirmOffset());
        this.haService.updateConfirmOffset(Math.min(confirmOffset, this.messageStore.getMaxPhyOffset()));
        System.out.println("client print confirm offset  after update: " + haService.getLocalAddress() + " " + haService.getConfirmOffset());

        // If epoch changed to bigger, last epoch record would be terminated
        if (this.currentReceivedEpoch < currentBlockEpoch) {
            System.out.println("put new epoch to self, client current=" + this.currentReceivedEpoch + ", block=" + currentBlockEpoch + ", block start=" + epochStartOffset);
            this.currentReceivedEpoch = currentBlockEpoch;
            this.epochCache.tryAppendEpochEntry(new EpochEntry(currentBlockEpoch, epochStartOffset));
        }

        this.currentTransferOffset = this.messageStore.getMaxPhyOffset();
    }
}

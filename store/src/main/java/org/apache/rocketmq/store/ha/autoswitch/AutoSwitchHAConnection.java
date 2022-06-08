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

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.ha.FlowMonitor;
import org.apache.rocketmq.store.ha.HAConnection;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.apache.rocketmq.store.ha.netty.HAMessage;
import org.apache.rocketmq.store.ha.netty.HAMessageType;
import org.apache.rocketmq.store.ha.protocol.PushCommitLogData;

public class AutoSwitchHAConnection implements HAConnection {

    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AutoSwitchHAService haService;

    private final EpochStore epochCache;

    // No need to put in queue
    private final Queue<PushCommitLogData> pendingQueue = new LinkedBlockingQueue<PushCommitLogData>();

    private final Channel channel;
    private final FlowMonitor flowMonitor;
    private long nextTransferFromWhere = -1;
    private SelectMappedBufferResult selectMappedBufferResult;
    private volatile long currentTransferOffset = 0;
    private volatile long currentTransferEpoch = -1;
    private volatile long currentTransferEpochEndOffset = 0;

    private volatile HAConnectionState currentState = HAConnectionState.HANDSHAKE;

    private volatile long slaveId = -1L;
    private volatile long slaveRequestOffset = -1;
    private volatile long slaveAckOffset = -1;
    private volatile long slaveAckTimestamp = -1;

    private volatile boolean slaveReadOnlyEnable = false;

    public AutoSwitchHAConnection(AutoSwitchHAService haService, Channel channel, EpochStore epochCache) {
        this.haService = haService;
        this.epochCache = epochCache;
        this.channel = channel;
        this.haService.getConnectionCount().incrementAndGet();
        this.flowMonitor = new FlowMonitor(this.haService.getDefaultMessageStore().getMessageStoreConfig());
    }

    @Override
    public void start() {
        changeCurrentState(HAConnectionState.HANDSHAKE);
        this.flowMonitor.start();
    }

    @Override
    public void shutdown() {
        changeCurrentState(HAConnectionState.SHUTDOWN);
        this.flowMonitor.shutdown(true);
        this.close();
    }

    @Override
    public void close() {

    }

    public void changeCurrentState(HAConnectionState connectionState) {
        LOGGER.info("change state to {}", connectionState);
        this.currentState = connectionState;
    }

    public long getSlaveId() {
        return slaveId;
    }

    public long getSlaveAckTimestamp() {
        return slaveAckTimestamp;
    }

    public void setSlaveAckTimestamp(long slaveAckTimestamp) {
        this.slaveAckTimestamp = slaveAckTimestamp;
    }

    public String getSlaveAddress() {
        return "slaveAddress";
    }

    @Override
    public HAConnectionState getCurrentState() {
        return currentState;
    }

    @Override
    public SocketChannel getSocketChannel() {
        return null;
    }

    @Override
    public String getClientAddress() {
        return "clientAddress";
    }

    @Override
    public long getSlaveAckOffset() {
        return slaveAckOffset;
    }

    @Override
    public long getTransferredByteInSecond() {
        return flowMonitor.getTransferredByteInSecond();
    }

    @Override
    public long getTransferFromWhere() {
        return 0L;
    }

    public void sendEpochToSlave() {
        HAMessage haMessage = new HAMessage(HAMessageType.RETURN_EPOCH, currentTransferEpoch,
            Objects.requireNonNull(RemotingSerializable.encode(epochCache.getAllEntries())));
        channel.writeAndFlush(haMessage);
    }

    class NettyTransferService extends ServiceThread {

        @Override
        public String getServiceName() {
            return null;
        }

        @Override
        public void run() {
            LOGGER.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                try {
                    switch (currentState) {
                        case HANDSHAKE:
                            this.waitForRunning(10);
                            continue;
                        case TRANSFER:
                            this.pushCommitLogDataToSlave();
                    }
                } catch (Exception e) {
                    LOGGER.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }
        }

        public void pushCommitLogDataToSlave() {
            if (-1 == slaveRequestOffset) {
                this.waitForRunning(10);
            }

            if (-1 == nextTransferFromWhere) {

                if (0 == slaveRequestOffset) {
                    // We must ensure that the starting point of syncing log
                    // must be the startOffset of a file (maybe the last file, or the minOffset)
                    nextTransferFromWhere = haService.getDefaultMessageStore().getCommitLog().getMinOffset();
                } else {
                    nextTransferFromWhere = slaveRequestOffset;
                }

                // Setup initial transferEpoch
                EpochEntry epochEntry = epochCache.findEpochEntryByOffset(nextTransferFromWhere);
                if (epochEntry == null) {
                    LOGGER.error("Failed to find an epochEntry to match slaveRequestOffset {}", nextTransferFromWhere);
                    waitForRunning(500);
                    return;
                }
                changeTransferEpochToNext(epochEntry);
                LOGGER.info("Master transfer data to slave {}, from offset:{}, currentEpoch:{}",
                    nextTransferFromWhere, epochEntry);
            }

            pushCommitLogDataToSlave0();
        }

        private void changeTransferEpochToNext(final EpochEntry entry) {
            currentTransferEpoch = entry.getEpoch();
            currentTransferEpochEndOffset = entry.getEndOffset();
            if (entry.getEpoch() == epochCache.getLastEpoch()) {
                // Use -1 to stand for Long.max
                currentTransferEpochEndOffset = -1;
            }
        }

        protected int getNextTransferDataSize() {
            DefaultMessageStore messageStore = haService.getDefaultMessageStore();
            if (currentTransferOffset >= messageStore.getMaxPhyOffset()) {
                return 0;
            }
            return 0;
        }

        private void pushCommitLogDataToSlave0() {
            int canTransferMaxBytes = flowMonitor.canTransferMaxByteNum();

            int size = this.getNextTransferDataSize();
            if (size > canTransferMaxBytes) {
                if (System.currentTimeMillis() - slaveAckTimestamp > 1000) {
                    LOGGER.warn("Trigger HA flow control, max transfer speed {}KB/s, current speed: {}KB/s",
                        String.format("%.2f", flowMonitor.maxTransferByteInSecond() / 1024.0),
                        String.format("%.2f", flowMonitor.getTransferredByteInSecond() / 1024.0));
                    slaveAckTimestamp = System.currentTimeMillis();
                }
                size = canTransferMaxBytes;
            }
            if (size <= 0) {
                this.releaseData();
            }

            // We must ensure that the transmitted logs are within the same epoch
            // If currentEpochEndOffset == -1, means that currentTransferEpoch = last epoch, so the endOffset = Long.max
            final long currentEpochEndOffset = currentTransferEpochEndOffset;
            if (currentEpochEndOffset != -1 && nextTransferFromWhere + size > currentEpochEndOffset) {
                final EpochEntry epochEntry = epochCache.findCeilingEntryByEpoch(currentTransferEpoch);
                if (epochEntry == null) {
                    LOGGER.error("Can't find a bigger epochEntry than epoch {}", currentTransferEpoch);
                    waitForRunning(100);
                    return;
                }
                size = (int) (currentEpochEndOffset - nextTransferFromWhere);
                changeTransferEpochToNext(epochEntry);
            }

            currentTransferOffset = nextTransferFromWhere;
            nextTransferFromWhere += size;
            doNettyTransferData();
        }

        private void doNettyTransferData() {
            HAMessage haMessage = new HAMessage(HAMessageType.PUSH_DATA);
            haMessage.setBody(selectMappedBufferResult.getByteBuffer());
            ChannelFuture future = channel.writeAndFlush(selectMappedBufferResult.getByteBuffer());
            future.addListener((ChannelFutureListener) future1 -> releaseData()).syncUninterruptibly();
        }

        protected void releaseData() {
            selectMappedBufferResult.release();
            selectMappedBufferResult = null;
        }
    }
}

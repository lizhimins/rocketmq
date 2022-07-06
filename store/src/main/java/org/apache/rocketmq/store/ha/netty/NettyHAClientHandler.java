package org.apache.rocketmq.store.ha.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.store.ha.HAConnectionState;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAClient;
import org.apache.rocketmq.store.ha.protocol.HandshakeMaster;

public class NettyHAClientHandler extends ChannelInboundHandlerAdapter {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AutoSwitchHAClient nettyHAClient;

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.out.println("exception: cause" + cause);
        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        System.out.println("client disconnect to server " + ctx.channel().id());
        nettyHAClient.changePromise(false);
        super.channelUnregistered(ctx);
    }

    public NettyHAClientHandler(AutoSwitchHAClient nettyHAClient) {
        this.nettyHAClient = nettyHAClient;
    }

    public void masterHandshake(HAMessage message) {
        HandshakeMaster handshakeMaster = RemotingSerializable.decode(message.getBytes(), HandshakeMaster.class);
        nettyHAClient.masterHandshake(handshakeMaster);
    }

    public void returnEpoch(HAMessage message) {
        int remaining = message.getByteBuffer().remaining();
        if (remaining % (3 * 8) != 0) {
            throw new RuntimeException();
        }
        int size = remaining / 24;
        List<EpochEntry> entryList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            EpochEntry entry = new EpochEntry();
            entry.setEpoch(message.getByteBuffer().getLong());
            entry.setStartOffset(message.getByteBuffer().getLong());
            entry.setEndOffset(message.getByteBuffer().getLong());
            entryList.add(entry);
        }
        nettyHAClient.doConsistencyRepairWithMaster(entryList);
    }

    public void pushData(HAMessage message) {
        long epoch = message.getByteBuffer().getLong();
        long confirmOffset = message.getByteBuffer().getLong();
        long startOffset = message.getByteBuffer().getLong();
        System.out.printf("receive data, epoch: %d, block start offset: %s, available payload size: %s%n",
            epoch, startOffset, message.getByteBuffer().remaining());
        nettyHAClient.doPutCommitLog(epoch, confirmOffset, startOffset, message.getByteBuffer());
        nettyHAClient.sendPushCommitLogAck();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        if (!(msg instanceof HAMessage)) {
            return;
        }

        HAMessage message = (HAMessage) msg;

        if (nettyHAClient.getCurrentMasterEpoch() != message.getEpoch()) {
            System.out.println("client epoch not match, connection epoch "
                + nettyHAClient.getCurrentMasterEpoch() + " " + message.getEpoch());
            log.error("epoch not match, connection epoch:{}", message.getEpoch());
            nettyHAClient.shutdown();
        }

        System.out.println(message.getType());
        switch (message.getType()) {
            case MASTER_HANDSHAKE:
                masterHandshake(message);
                break;
            case RETURN_EPOCH:
                returnEpoch(message);
                break;
            case PUSH_DATA:
                pushData(message);
                break;
            default:
                System.out.println("invalid message");
                log.info("receive invalid message, ", message.getType());
        }
    }
}

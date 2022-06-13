package org.apache.rocketmq.store.ha.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAClient;
import org.apache.rocketmq.store.ha.protocol.HandshakeMaster;

public class NettyHAClientHandler extends ChannelInboundHandlerAdapter {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AutoSwitchHAClient nettyHAClient;

    public NettyHAClientHandler(AutoSwitchHAClient nettyHAClient) {
        this.nettyHAClient = nettyHAClient;
    }

    public void masterHandshake(HAMessage message) {
        HandshakeMaster handshakeMaster = RemotingSerializable.decode(message.getBytes(), HandshakeMaster.class);
        nettyHAClient.masterHandshake(handshakeMaster);
    }

    public void returnEpoch(HAMessage message) {
        List entryList = RemotingSerializable.decode(message.getByteBuffer().array(), List.class);
        nettyHAClient.doConsistencyRepairWithMaster(entryList);
    }

    public void pushData(HAMessage message) {
        long epoch = message.getByteBuffer().getLong();
        long startOffset = message.getByteBuffer().getLong();
        System.out.printf("receive data, block start offset: %s, available payload size: %s%n",
            startOffset, message.getByteBuffer().remaining());
        nettyHAClient.doPutCommitLog(epoch, startOffset, message.getByteBuffer());
        nettyHAClient.sendPushCommitLogAck(startOffset + message.getByteBuffer().limit() - 16);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        if (!(msg instanceof HAMessage)) {
            return;
        }

        HAMessage message = (HAMessage) msg;

        if (nettyHAClient.getCurrentMasterEpoch() != message.getEpoch()) {
            System.out.println("client epoch not match, connection epoch " + message.getEpoch());
            log.error("epoch not match, connection epoch:{}", message.getEpoch());
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

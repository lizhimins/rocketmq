package org.apache.rocketmq.store.ha.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.ha.protocol.ConfirmTruncate;
import org.apache.rocketmq.store.ha.protocol.HandshakeMaster;
import org.apache.rocketmq.store.ha.protocol.HandshakeResult;
import org.apache.rocketmq.store.ha.protocol.HandshakeSlave;
import org.apache.rocketmq.store.ha.protocol.PushCommitLogData;

public class NettyHAServerHandler extends SimpleChannelInboundHandler<HAMessage> {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final AutoSwitchHAService nettyHAService;

    public NettyHAServerHandler(AutoSwitchHAService autoSwitchHAService) {
        this.nettyHAService = autoSwitchHAService;
    }

    public void slaveHandshake(HAMessage message, Channel channel) {
        HandshakeSlave handshakeSlave = RemotingSerializable.decode(message.getByteBuffer().array(), HandshakeSlave.class);
        HandshakeResult handshakeResult = nettyHAService.checkSlaveIdentity(handshakeSlave);
        HandshakeMaster handshakeMaster = nettyHAService.replyHandshakeToSlave(handshakeResult);
        byte[] encode = RemotingSerializable.encode(handshakeMaster);
        assert encode != null;
        HAMessage replyMessage = new HAMessage(
            HAMessageType.MASTER_HANDSHAKE, nettyHAService.getCurrentMasterEpoch(), encode);
        channel.writeAndFlush(replyMessage);
    }

    public void responseEpochList(Channel channel) {
        byte[] encode = RemotingSerializable.encode(nettyHAService.getEpochEntries());
        assert encode != null;
        HAMessage replyMessage = new HAMessage(
            HAMessageType.RETURN_EPOCH, nettyHAService.getCurrentMasterEpoch(), encode);
        channel.writeAndFlush(replyMessage);
    }

    /**
     * Master change state to transfer and start push data to slave
     */
    public void confirmTruncate(HAMessage message, Channel channel) {
        ConfirmTruncate confirmTruncate = RemotingSerializable.decode(message.getByteBuffer().array(), ConfirmTruncate.class);
        nettyHAService.confirmTruncate(channel.remoteAddress().toString(), confirmTruncate.getCommitLogStartOffset());
    }

    public void pushCommitLogAck(HAMessage message, Channel channel) {
        PushCommitLogData pushCommitLogData = RemotingSerializable.decode(message.getByteBuffer().array(), PushCommitLogData.class);
        nettyHAService.pushAck(channel.remoteAddress().toString(), pushCommitLogData.getStartOffset());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HAMessage message) throws Exception {
        if (message == null || message.getType() == null) {
            return;
        }

        if (nettyHAService.getCurrentMasterEpoch() != message.getEpoch()) {
            System.out.println("epoch not match, connection epoch " + message.getEpoch());
            log.error("epoch not match, connection epoch:{}", message.getEpoch());
        }

        Channel channel = ctx.channel();
        switch (message.getType()) {
            case SLAVE_HANDSHAKE:
                slaveHandshake(message, channel);
                break;
            case QUERY_EPOCH:
                responseEpochList(channel);
                break;
            case CONFIRM_TRUNCATE:
                confirmTruncate(message, channel);
                break;
            case PUSH_ACK:
                pushCommitLogAck(message, channel);
                break;
            default:
                System.out.println("invalid message");
                log.info("receive invalid message, ", message.getType());
        }
    }
}

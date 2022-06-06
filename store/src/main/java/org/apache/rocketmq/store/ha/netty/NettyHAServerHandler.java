package org.apache.rocketmq.store.ha.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.List;
import org.apache.rocketmq.common.EpochEntry;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;

public class NettyHAServerHandler extends SimpleChannelInboundHandler<HAMessage> {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private AutoSwitchHAService autoSwitchHAService;

    public NettyHAServerHandler(AutoSwitchHAService autoSwitchHAService) {
        this.autoSwitchHAService = autoSwitchHAService;
    }

    public void slaveHandshake(HAMessage message, Channel channel) {

    }

    public void queryEpoch(HAMessage message, Channel channel) {
        List<EpochEntry> entries = autoSwitchHAService.getEpochEntries();
        //channel.writeAndFlush(new HAMessage(), new DefaultChannelPromise());
    }

    public void confirmTruncate(HAMessage message, Channel channel) {
        // start push data to slave
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HAMessage message) throws Exception {
        if (message == null || message.getType() == null) {
            return;
        }

        Channel channel = ctx.channel();
        switch (message.getType()) {
            case SLAVE_HANDSHAKE:
                slaveHandshake(message, channel);
                break;
            case QUERY_EPOCH:
                queryEpoch(message, channel);
                break;
            case CONFIRM_TRUNCATE:
                confirmTruncate(message, channel);
                break;
            case PUSH_ACK:

                break;
            default:
                System.out.println("invalid message");
                log.info("receive invalid message, ", message.getType());
        }
    }
}

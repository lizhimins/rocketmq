package org.apache.rocketmq.store.ha.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class NettyHADecoder extends LengthFieldBasedFrameDecoder {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final int FRAME_MAX_LENGTH =
        Integer.parseInt(System.getProperty("com.rocketmq.remoting.frameMaxLength", "16777216"));

    private long firstReceiveEpoch = -1L;

    private long lastReadTimestamp = System.currentTimeMillis();

    public NettyHADecoder() {
        super(FRAME_MAX_LENGTH, 0, 4, 16, 0);
    }

    public long getLastReadTimestamp() {
        return lastReadTimestamp;
    }

    @Override
    protected HAMessage decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            return null;
        }

        int bodyLength = frame.readInt();
        HAMessageType messageType = HAMessageType.valueOf(frame.readInt());
        long epoch = frame.readLong();
        if (firstReceiveEpoch == -1) {
            firstReceiveEpoch = epoch;
        }

        if (firstReceiveEpoch != epoch) {
            log.error("epoch not match");
        }

        long timestamp = frame.readLong();
        ByteBuf byteBuf = frame.readBytes(bodyLength);
        return new HAMessage(messageType, timestamp, byteBuf);
    }
}

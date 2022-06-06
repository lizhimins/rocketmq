package org.apache.rocketmq.store.ha.netty;

import com.alibaba.fastjson.JSONObject;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.store.ha.protocol.HandshakeMaster;

@ChannelHandler.Sharable
public class NettyHAClientHandler extends ChannelInboundHandlerAdapter {

    private final AtomicBoolean firstReceiveStatus = new AtomicBoolean(false);
    private ChannelHandlerContext ctx;
    private ChannelPromise channelPromise;
    private HandshakeMaster handshakeMaster;
    private final AtomicBoolean first = new AtomicBoolean(false);

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
    }

    public void masterHandshake(HAMessage message) {
        byte[] bytes = new byte[message.getByteBuf().readableBytes()];
        message.getByteBuf().readBytes(bytes);
        handshakeMaster = JSONObject.parseObject(bytes, HandshakeMaster.class);
        channelPromise.setSuccess();
    }

    public void returnEpoch(HAMessage message) {
        byte[] bytes = new byte[message.getByteBuf().readableBytes()];
        message.getByteBuf().readBytes(bytes);
//            masterStatus = JSONObject.parseObject(bytes, EpochQuery.class);

        // 更新备视角下主的状态，主要是更新 confirm offset 和 max offset
        if (first.compareAndSet(false, true)) {
            // System.out.println("receive");
            channelPromise.setSuccess();
        } else {
            // 非第一次更新
//                System.out.printf("receive master status, %s%n", masterStatus);
        }
    }

    public void pushData(HAMessage message) {

    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        if (!(msg instanceof HAMessage)) {
            return;
        }

        HAMessage message = (HAMessage) msg;

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
                break;
        }
    }

    public synchronized ChannelPromise sendMessage(HAMessage message) {
        while (ctx == null) {
            try {
                TimeUnit.MILLISECONDS.sleep(10);
                System.out.println("waiting");
            } catch (InterruptedException e) {
                System.out.println("等待ChannelHandlerContext实例化过程中出错" + e);
            }
        }
        channelPromise = ctx.newPromise();
        ctx.writeAndFlush(message);
        return channelPromise;
    }

    public HandshakeMaster getAddSlaveResponse() {
        return handshakeMaster;
    }
}

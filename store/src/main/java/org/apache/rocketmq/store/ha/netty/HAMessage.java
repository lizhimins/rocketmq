package org.apache.rocketmq.store.ha.netty;

import io.netty.buffer.ByteBuf;

public class HAMessage {

    // 消息长度
    private int length;

    // 消息类型
    private HAMessageType type;

    private long epoch;

    // 消息体
    private byte[] body;

    private ByteBuf byteBuf;

    public HAMessage() {
    }

    public HAMessage(HAMessageType type) {
        this.type = type;
    }

    public HAMessage(HAMessageType type, byte[] body) {
        this.type = type;
        this.length = body.length;
        this.body = body;
    }

    public HAMessage(HAMessageType type, long timestamp, byte[] body) {
        this.type = type;
        this.body = body;
    }

    public HAMessage(HAMessageType type, long timestamp, ByteBuf byteBuf) {
        this.type = type;
        this.byteBuf = byteBuf;
    }

    public HAMessageType getType() {
        return type;
    }

    public void setType(HAMessageType type) {
        this.type = type;
    }

    public int getLength() {
        return length;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
        this.length = body.length;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public ByteBuf getByteBuf() {

        return byteBuf;
    }

    public void setByteBuf(ByteBuf byteBuf) {
        this.byteBuf = byteBuf;
    }

    @Override
    public String toString() {
        return "HAMessage{" +
            "length=" + length +
            ", type=" + type +
            '}';
    }
}

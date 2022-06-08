package org.apache.rocketmq.store.ha.netty;

import java.nio.ByteBuffer;

public class HAMessage {

    private HAMessageType type;

    private long epoch;

    private ByteBuffer byteBuffer;

    private int bodyLength;

    public HAMessage(HAMessageType type) {
        this.type = type;
    }

    public HAMessage(HAMessageType type, long epoch, byte[] bytes) {
        this.type = type;
        this.epoch = epoch;
        this.setBody(bytes);
    }

    public HAMessage(HAMessageType type, long epoch, ByteBuffer byteBuffer) {
        this.type = type;
        this.epoch = epoch;
        this.setBody(byteBuffer);
    }

    public HAMessageType getType() {
        return type;
    }

    public void setType(HAMessageType type) {
        this.type = type;
    }

    public long getEpoch() {
        return epoch;
    }

    public void setEpoch(long epoch) {
        this.epoch = epoch;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public void setBody(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
        this.bodyLength = byteBuffer.remaining();
    }

    public void setBody(byte[] bytes) {
        this.byteBuffer = ByteBuffer.allocate(bytes.length);
        this.byteBuffer.put(bytes);
        this.bodyLength = this.byteBuffer.remaining();
    }

    public int getBodyLength() {
        return bodyLength;
    }
}

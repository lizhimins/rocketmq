package org.apache.rocketmq.store.ha.protocol;

import org.apache.rocketmq.remoting.protocol.LanguageCode;

public class HandshakeMaster {

    private String clusterName;

    private String brokerName;

    private int brokerId;

    private int brokerAppVersion;

    private int haProtocolVersion;

    private LanguageCode languageCode;

    private HandshakeResult handshakeResult;

    private long commitLogMinOffset;

    private long commitLogMaxOffset;

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public int getBrokerAppVersion() {
        return brokerAppVersion;
    }

    public void setBrokerAppVersion(int brokerAppVersion) {
        this.brokerAppVersion = brokerAppVersion;
    }

    public int getHaProtocolVersion() {
        return haProtocolVersion;
    }

    public void setHaProtocolVersion(int haProtocolVersion) {
        this.haProtocolVersion = haProtocolVersion;
    }

    public LanguageCode getLanguageCode() {
        return languageCode;
    }

    public void setLanguageCode(LanguageCode languageCode) {
        this.languageCode = languageCode;
    }

    public HandshakeResult getHandshakeResult() {
        return handshakeResult;
    }

    public void setHandshakeResult(HandshakeResult handshakeResult) {
        this.handshakeResult = handshakeResult;
    }

    @Override
    public String toString() {
        return "HandshakeMaster{" +
            "clusterName='" + clusterName + '\'' +
            ", brokerName='" + brokerName + '\'' +
            ", brokerId=" + brokerId +
            ", brokerAppVersion=" + brokerAppVersion +
            ", haProtocolVersion=" + haProtocolVersion +
            ", languageCode=" + languageCode +
            ", handshakeResult=" + handshakeResult +
            '}';
    }
}

package com.wheelseye.devicegateway.domain.events;

import com.wheelseye.devicegateway.domain.valueobjects.IMEI;
import java.time.Instant;

public class DeviceSessionEvent {
    private final String eventType;
    private final String sessionId;
    private final IMEI imei;
    private final String deviceModel;
    private final String ipAddress;
    private final Instant timestamp;
    private final String protocolVersion;

    public DeviceSessionEvent(String eventType, String sessionId, IMEI imei, 
                             String deviceModel, String ipAddress, String protocolVersion) {
        this.eventType = eventType;
        this.sessionId = sessionId;
        this.imei = imei;
        this.deviceModel = deviceModel;
        this.ipAddress = ipAddress;
        this.protocolVersion = protocolVersion;
        this.timestamp = Instant.now();
    }

    // Getters
    public String getEventType() { return eventType; }
    public String getSessionId() { return sessionId; }
    public IMEI getImei() { return imei; }
    public String getDeviceModel() { return deviceModel; }
    public String getIpAddress() { return ipAddress; }
    public Instant getTimestamp() { return timestamp; }
    public String getProtocolVersion() { return protocolVersion; }

    // Static factory methods
    public static DeviceSessionEvent connected(String sessionId, IMEI imei, String deviceModel, String ipAddress) {
        return new DeviceSessionEvent("SESSION_CONNECTED", sessionId, imei, deviceModel, ipAddress, "1.2");
    }

    public static DeviceSessionEvent disconnected(String sessionId, IMEI imei, String deviceModel, String ipAddress) {
        return new DeviceSessionEvent("SESSION_DISCONNECTED", sessionId, imei, deviceModel, ipAddress, "1.2");
    }
}

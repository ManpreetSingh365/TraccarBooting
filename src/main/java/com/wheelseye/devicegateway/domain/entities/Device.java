package com.wheelseye.devicegateway.domain.entities;

import com.wheelseye.devicegateway.domain.valueobjects.IMEI;
import java.time.Instant;

public class Device {
    private final IMEI imei;
    private DeviceStatus status;
    private String model;
    private String firmwareVersion;
    private Instant lastSeen;
    private String ipAddress;

    public Device(IMEI imei, String model, String firmwareVersion) {
        this.imei = imei;
        this.model = model;
        this.firmwareVersion = firmwareVersion;
        this.status = DeviceStatus.NEW;
        this.lastSeen = Instant.now();
    }

    // Getters and business methods
    public IMEI getImei() { return imei; }
    public DeviceStatus getStatus() { return status; }
    public String getModel() { return model; }
    public String getFirmwareVersion() { return firmwareVersion; }
    public Instant getLastSeen() { return lastSeen; }
    public String getIpAddress() { return ipAddress; }

    public void updateLastSeen() {
        this.lastSeen = Instant.now();
    }

    public void activate() {
        this.status = DeviceStatus.ACTIVE;
    }

    public void deactivate() {
        this.status = DeviceStatus.INACTIVE;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }
}

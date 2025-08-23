package com.wheelseye.devicegateway.domain.entities;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import com.wheelseye.devicegateway.domain.valueobjects.IMEI;

import io.netty.channel.Channel;

public class DeviceSession {
    private final String sessionId;
    private final IMEI imei;
    private final Channel channel;
    private final Instant connectedAt;
    private Instant lastActivity;
    private final AtomicInteger messageSequence;
    private boolean authenticated;

    public DeviceSession(String sessionId, IMEI imei, Channel channel) {
        this.sessionId = sessionId;
        this.imei = imei;
        this.channel = channel;
        this.connectedAt = Instant.now();
        this.lastActivity = Instant.now();
        this.messageSequence = new AtomicInteger(0);
        this.authenticated = false;
    }

    public String getSessionId() { return sessionId; }
    public IMEI getImei() { return imei; }
    public Channel getChannel() { return channel; }
    public Instant getConnectedAt() { return connectedAt; }
    public Instant getLastActivity() { return lastActivity; }
    public boolean isAuthenticated() { return authenticated; }
    
    public void updateActivity() {
        this.lastActivity = Instant.now();
    }
    
    public void authenticate() {
        this.authenticated = true;
    }
    
    public int getNextSequence() {
        return messageSequence.incrementAndGet();
    }
    
    public boolean isIdle(long maxIdleSeconds) {
        return Instant.now().getEpochSecond() - lastActivity.getEpochSecond() > maxIdleSeconds;
    }
}

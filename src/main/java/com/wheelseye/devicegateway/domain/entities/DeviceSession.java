package com.wheelseye.devicegateway.domain.entities;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;

/**
 * DeviceSession Entity - Fixed for Redis serialization
 * 
 * Key Fixes:
 * 1. ✅ Proper Jackson annotations for serialization
 * 2. ✅ Channel management improvements
 * 3. ✅ Thread-safe operations
 * 4. ✅ Better activity tracking
 */
public class DeviceSession {
    
    private final String id;
    private IMEI imei;
    private String channelId;  // Store channel ID instead of Channel object
    private Instant createdAt;
    private Instant lastActivityAt;
    private boolean authenticated;
    private String remoteAddress;
    private final Map<String, Object> attributes;
    
    // Default constructor
    public DeviceSession() {
        this.id = UUID.randomUUID().toString();
        this.createdAt = Instant.now();
        this.lastActivityAt = Instant.now();
        this.authenticated = false;
        this.attributes = new HashMap<>();
    }
    
    // Constructor with ID and IMEI
    public DeviceSession(String id, IMEI imei) {
        this.id = id != null ? id : UUID.randomUUID().toString();
        this.imei = imei;
        this.createdAt = Instant.now();
        this.lastActivityAt = Instant.now();
        this.authenticated = false;
        this.attributes = new HashMap<>();
    }
    
    // Constructor with all fields for Jackson deserialization
    @JsonCreator
    public DeviceSession(
        @JsonProperty("id") String id,
        @JsonProperty("imei") IMEI imei,
        @JsonProperty("channelId") String channelId,
        @JsonProperty("createdAt") Instant createdAt,
        @JsonProperty("lastActivityAt") Instant lastActivityAt,
        @JsonProperty("authenticated") boolean authenticated,
        @JsonProperty("remoteAddress") String remoteAddress,
        @JsonProperty("attributes") Map<String, Object> attributes
    ) {
        this.id = id != null ? id : UUID.randomUUID().toString();
        this.imei = imei;
        this.channelId = channelId;
        this.createdAt = createdAt != null ? createdAt : Instant.now();
        this.lastActivityAt = lastActivityAt != null ? lastActivityAt : Instant.now();
        this.authenticated = authenticated;
        this.remoteAddress = remoteAddress;
        this.attributes = attributes != null ? new HashMap<>(attributes) : new HashMap<>();
    }
    
    /**
     * Create session from IMEI - factory method
     */
    public static DeviceSession create(IMEI imei) {
        return new DeviceSession(UUID.randomUUID().toString(), imei);
    }
    
    /**
     * Update activity timestamp - thread safe
     */
    public synchronized void updateActivity() {
        this.lastActivityAt = Instant.now();
    }
    
    /**
     * Authenticate the session
     */
    public synchronized void authenticate() {
        this.authenticated = true;
        updateActivity();
    }
    
    /**
     * Check if session is idle
     */
    public boolean isIdle(long maxIdleSeconds) {
        return Instant.now().isAfter(lastActivityAt.plusSeconds(maxIdleSeconds));
    }
    
    /**
     * Get session duration in seconds
     */
    @JsonIgnore
    public long getDurationSeconds() {
        return Instant.now().getEpochSecond() - createdAt.getEpochSecond();
    }
    
    /**
     * Get idle time in seconds
     */
    @JsonIgnore
    public long getIdleTimeSeconds() {
        return Instant.now().getEpochSecond() - lastActivityAt.getEpochSecond();
    }
    
    /**
     * Set attribute with type safety
     */
    public synchronized void setAttribute(String key, Object value) {
        if (key != null) {
            attributes.put(key, value);
            updateActivity();
        }
    }
    
    /**
     * Get attribute with default value
     */
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(String key, T defaultValue) {
        Object value = attributes.get(key);
        if (value != null) {
            try {
                return (T) value;
            } catch (ClassCastException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }
    
    /**
     * Remove attribute
     */
    public synchronized Object removeAttribute(String key) {
        return attributes.remove(key);
    }
    
    // Getters
    public String getId() { return id; }
    
    public IMEI getImei() { return imei; }
    
    public String getChannelId() { return channelId; }
    
    public Instant getCreatedAt() { return createdAt; }
    
    public Instant getLastActivityAt() { return lastActivityAt; }
    
    public boolean isAuthenticated() { return authenticated; }
    
    public String getRemoteAddress() { return remoteAddress; }
    
    public Map<String, Object> getAttributes() { 
        return new HashMap<>(attributes); // Return copy for thread safety
    }
    
    // Setters
    public void setImei(IMEI imei) {
        this.imei = imei;
        updateActivity();
    }
    
    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }
    
    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }
    
    public void setLastActivityAt(Instant lastActivityAt) {
        this.lastActivityAt = lastActivityAt;
    }
    
    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }
    
    public void setRemoteAddress(String remoteAddress) {
        this.remoteAddress = remoteAddress;
    }
    
    // Utility methods
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeviceSession that = (DeviceSession) o;
        return id.equals(that.id);
    }
    
    @Override
    public int hashCode() {
        return id.hashCode();
    }
    
    @Override
    public String toString() {
        return String.format("DeviceSession{id='%s', imei=%s, authenticated=%s, channelId='%s', remoteAddress='%s'}",
            id, imei != null ? imei.getValue() : null, authenticated, channelId, remoteAddress);
    }
}
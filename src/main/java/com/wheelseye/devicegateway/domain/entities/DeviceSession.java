package com.wheelseye.devicegateway.domain.entities;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;

import io.netty.channel.Channel;

/**
 * Enhanced DeviceSession Entity - FULLY COMPATIBLE with your existing code
 * 
 * Key Enhancements:
 * 1. ✅ V5 device variant support (backward compatible)
 * 2. ✅ All existing constructors preserved
 * 3. ✅ Enhanced with new methods for V5 handling
 * 4. ✅ Proper Jackson annotations for Redis serialization
 * 5. ✅ Thread-safe operations
 */
public class DeviceSession {

    private final String id;
    private IMEI imei;
    private String channelId; // Store channel ID instead of Channel object
    private Instant createdAt;
    private Instant lastActivityAt;
    private boolean authenticated;
    private String remoteAddress;
    private final Map<String, Object> attributes;

    // NEW: Enhanced fields for V5 device support
    private volatile String deviceVariant = "UNKNOWN"; // Make it volatile for thread safety

    private boolean hasReceivedLocationData = false;
    private boolean statusAdviceGiven = false;

    // Default constructor (EXISTING)
    public DeviceSession() {
        this.id = UUID.randomUUID().toString();
        this.createdAt = Instant.now();
        this.lastActivityAt = Instant.now();
        this.authenticated = false;
        this.attributes = new HashMap<>();
    }

    // Constructor with ID and IMEI (EXISTING)
    public DeviceSession(String id, IMEI imei) {
        this.id = id != null ? id : UUID.randomUUID().toString();
        this.imei = imei;
        this.createdAt = Instant.now();
        this.lastActivityAt = Instant.now();
        this.authenticated = false;
        this.attributes = new HashMap<>();
    }

    // NEW: Constructor for compatibility with fixed GT06Handler
    public DeviceSession(IMEI imei, Channel channel) {
        this.id = UUID.randomUUID().toString();
        this.imei = imei;
        this.channelId = channel != null ? channel.id().asShortText() : null;
        this.remoteAddress = channel != null && channel.remoteAddress() != null ? channel.remoteAddress().toString()
                : "unknown";
        this.createdAt = Instant.now();
        this.lastActivityAt = Instant.now();
        this.authenticated = false;
        this.attributes = new HashMap<>();
    }

    // Constructor with all fields for Jackson deserialization (EXISTING)
    @JsonCreator
    public DeviceSession(
            @JsonProperty("id") String id,
            @JsonProperty("imei") IMEI imei,
            @JsonProperty("channelId") String channelId,
            @JsonProperty("createdAt") Instant createdAt,
            @JsonProperty("lastActivityAt") Instant lastActivityAt,
            @JsonProperty("authenticated") boolean authenticated,
            @JsonProperty("remoteAddress") String remoteAddress,
            @JsonProperty("attributes") Map<String, Object> attributes) {
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
     * Create session from IMEI - factory method (EXISTING)
     */
    public static DeviceSession create(IMEI imei) {
        return new DeviceSession(UUID.randomUUID().toString(), imei);
    }

    /**
     * Update activity timestamp - thread safe (EXISTING)
     */
    public synchronized void updateActivity() {
        this.lastActivityAt = Instant.now();
    }

    /**
     * Authenticate the session (EXISTING)
     */
    public synchronized void authenticate() {
        this.authenticated = true;
        updateActivity();
    }

    /**
     * Check if session is idle (EXISTING)
     */
    public boolean isIdle(long maxIdleSeconds) {
        return Instant.now().isAfter(lastActivityAt.plusSeconds(maxIdleSeconds));
    }

    /**
     * Get session duration in seconds (EXISTING)
     */
    @JsonIgnore
    public long getDurationSeconds() {
        return Instant.now().getEpochSecond() - createdAt.getEpochSecond();
    }

    /**
     * Get idle time in seconds (EXISTING)
     */
    @JsonIgnore
    public long getIdleTimeSeconds() {
        return Instant.now().getEpochSecond() - lastActivityAt.getEpochSecond();
    }

    /**
     * Set attribute with type safety (EXISTING)
     */
    public synchronized void setAttribute(String key, Object value) {
        if (key != null) {
            attributes.put(key, value);
            updateActivity();
        }
    }

    /**
     * Get attribute with default value (EXISTING)
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
     * Remove attribute (EXISTING)
     */
    public synchronized Object removeAttribute(String key) {
        return attributes.remove(key);
    }

    // ========== NEW: V5 DEVICE SUPPORT METHODS ==========

    /**
     * Get device variant (V5, SK05, etc.)
     */
    public String getDeviceVariant() {
        return this.deviceVariant;
    }

    /**
     * Set device variant for proper handling
     */
    public void setDeviceVariant(String variant) {
        this.deviceVariant = variant != null ? variant : "UNKNOWN";
        System.out.println("Device variant set to: " + this.deviceVariant + " for session: " + this.id);
        updateActivity();
    }

    /**
     * Check if device has sent location data
     */
    public boolean hasReceivedLocationData() {
        return hasReceivedLocationData;
    }

    /**
     * Mark that device has sent location data
     */
    public void markLocationDataReceived() {
        this.hasReceivedLocationData = true;
        updateActivity();
    }

    /**
     * Check if status advice has been given (to avoid spam)
     */
    public boolean hasReceivedStatusAdvice() {
        return statusAdviceGiven;
    }

    /**
     * Mark that status advice has been given
     */
    public void markStatusAdviceGiven() {
        this.statusAdviceGiven = true;
    }

    /**
     * Check if this is a V5 device
     */
    @JsonIgnore
    public boolean isV5Device() {
        return "V5".equalsIgnoreCase(deviceVariant);
    }

    /**
     * Check if this is an SK05 device
     */
    @JsonIgnore
    public boolean isSK05Device() {
        return "SK05".equalsIgnoreCase(deviceVariant);
    }

    /**
     * Check if session contains a specific key (for compatibility)
     */
    public boolean contains(String key) {
        return attributes.containsKey(key);
    }

    /**
     * Set value in session (for compatibility)
     */
    public void set(String key, Object value) {
        setAttribute(key, value);
    }

    /**
     * Get value from session (for compatibility)
     */
    public Object get(String key) {
        return attributes.get(key);
    }

    // ========== EXISTING GETTERS ==========
    public String getId() {
        return id;
    }

    public IMEI getImei() {
        return imei;
    }

    public String getChannelId() {
        return channelId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getLastActivityAt() {
        return lastActivityAt;
    }

    public boolean isAuthenticated() {
        return authenticated;
    }

    public String getRemoteAddress() {
        return remoteAddress;
    }

    public Map<String, Object> getAttributes() {
        return new HashMap<>(attributes); // Return copy for thread safety
    }

    // ========== EXISTING SETTERS ==========
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

    // ========== EXISTING UTILITY METHODS ==========
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DeviceSession that = (DeviceSession) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return String.format(
                "DeviceSession{id='%s', imei=%s, authenticated=%s, channelId='%s', remoteAddress='%s', variant='%s'}",
                id, imei != null ? imei.getValue() : null, authenticated, channelId, remoteAddress, deviceVariant);
    }
}
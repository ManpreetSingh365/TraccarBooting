package com.wheelseye.devicegateway.infrastructure.persistence;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;
import com.wheelseye.devicegateway.domain.entities.DeviceSession;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.Channel;

/**
 * Redis Session Repository - Fixed for proper serialization/deserialization
 * 
 * Key Fixes:
 * 1. ✅ Proper SessionData class with Jackson annotations
 * 2. ✅ Handles LinkedHashMap to SessionData conversion
 * 3. ✅ Robust error handling and logging
 * 4. ✅ Channel ID based session management
 * 5. ✅ Proper cleanup and timeout handling
 */
@Repository
public class RedisSessionRepository {
    
    private static final Logger logger = LoggerFactory.getLogger(RedisSessionRepository.class);
    
    private static final String SESSION_KEY_PREFIX = "device:session:";
    private static final String CHANNEL_SESSION_PREFIX = "channel:session:";
    private static final String IMEI_SESSION_PREFIX = "imei:session:";
    private static final String ACTIVE_SESSIONS_SET = "active:sessions";
    
    private static final long SESSION_TTL_SECONDS = 3600; // 1 hour
    
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    
    @Autowired
    private ObjectMapper redisObjectMapper;
    
    /**
     * Fixed SessionData class with proper Jackson serialization support
     */
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
    public static class SessionData {
        private String sessionId;
        private String imei;
        private String channelId;
        private Instant createdAt;
        private Instant lastActivityAt;
        private boolean authenticated;
        private String remoteAddress;
        private Map<String, Object> attributes;
        
        // Default constructor for Jackson
        public SessionData() {
            this.attributes = new HashMap<>();
        }
        
        // Constructor with JsonCreator for proper deserialization
        @JsonCreator
        public SessionData(
            @JsonProperty("sessionId") String sessionId,
            @JsonProperty("imei") String imei,
            @JsonProperty("channelId") String channelId,
            @JsonProperty("createdAt") Instant createdAt,
            @JsonProperty("lastActivityAt") Instant lastActivityAt,
            @JsonProperty("authenticated") boolean authenticated,
            @JsonProperty("remoteAddress") String remoteAddress,
            @JsonProperty("attributes") Map<String, Object> attributes
        ) {
            this.sessionId = sessionId;
            this.imei = imei;
            this.channelId = channelId;
            this.createdAt = createdAt;
            this.lastActivityAt = lastActivityAt;
            this.authenticated = authenticated;
            this.remoteAddress = remoteAddress;
            this.attributes = attributes != null ? attributes : new HashMap<>();
        }
        
        // Create from DeviceSession
        public static SessionData fromDeviceSession(DeviceSession session) {
            return new SessionData(
                session.getId(),
                session.getImei() != null ? session.getImei().getValue() : null,
                session.getChannelId(),
                session.getCreatedAt(),
                session.getLastActivityAt(),
                session.isAuthenticated(),
                session.getRemoteAddress(),
                new HashMap<>(session.getAttributes())
            );
        }
        
        // Convert to DeviceSession
        public DeviceSession toDeviceSession() {
            IMEI imeiObj = imei != null ? new IMEI(imei) : null;
            DeviceSession session = new DeviceSession(sessionId, imeiObj);
            session.setChannelId(channelId);
            session.setCreatedAt(createdAt);
            session.setLastActivityAt(lastActivityAt);
            session.setAuthenticated(authenticated);
            session.setRemoteAddress(remoteAddress);
            session.getAttributes().putAll(attributes);
            return session;
        }
        
        // Getters and Setters
        public String getSessionId() { return sessionId; }
        public void setSessionId(String sessionId) { this.sessionId = sessionId; }
        
        public String getImei() { return imei; }
        public void setImei(String imei) { this.imei = imei; }
        
        public String getChannelId() { return channelId; }
        public void setChannelId(String channelId) { this.channelId = channelId; }
        
        public Instant getCreatedAt() { return createdAt; }
        public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
        
        public Instant getLastActivityAt() { return lastActivityAt; }
        public void setLastActivityAt(Instant lastActivityAt) { this.lastActivityAt = lastActivityAt; }
        
        public boolean isAuthenticated() { return authenticated; }
        public void setAuthenticated(boolean authenticated) { this.authenticated = authenticated; }
        
        public String getRemoteAddress() { return remoteAddress; }
        public void setRemoteAddress(String remoteAddress) { this.remoteAddress = remoteAddress; }
        
        public Map<String, Object> getAttributes() { return attributes; }
        public void setAttributes(Map<String, Object> attributes) { this.attributes = attributes; }
    }
    
    /**
     * Save session with proper error handling
     */
    public void save(DeviceSession session) {
        try {
            SessionData sessionData = SessionData.fromDeviceSession(session);
            String sessionKey = SESSION_KEY_PREFIX + session.getId();
            
            // Save session data
            redisTemplate.opsForValue().set(sessionKey, sessionData, SESSION_TTL_SECONDS, TimeUnit.SECONDS);
            
            // Index by channel ID
            if (session.getChannelId() != null) {
                String channelKey = CHANNEL_SESSION_PREFIX + session.getChannelId();
                redisTemplate.opsForValue().set(channelKey, session.getId(), SESSION_TTL_SECONDS, TimeUnit.SECONDS);
            }
            
            // Index by IMEI
            if (session.getImei() != null) {
                String imeiKey = IMEI_SESSION_PREFIX + session.getImei().getValue();
                redisTemplate.opsForValue().set(imeiKey, session.getId(), SESSION_TTL_SECONDS, TimeUnit.SECONDS);
            }
            
            // Add to active sessions set
            redisTemplate.opsForSet().add(ACTIVE_SESSIONS_SET, session.getId());
            redisTemplate.expire(ACTIVE_SESSIONS_SET, SESSION_TTL_SECONDS, TimeUnit.SECONDS);
            
            logger.debug("✅ Session saved successfully: {}", session.getId());
            
        } catch (Exception e) {
            logger.error("❌ Failed to save session: {}", session.getId(), e);
            throw new RuntimeException("Failed to save session", e);
        }
    }
    
    /**
     * Find session by ID with proper type conversion handling
     */
    public Optional<DeviceSession> findById(String sessionId) {
        try {
            String sessionKey = SESSION_KEY_PREFIX + sessionId;
            Object rawData = redisTemplate.opsForValue().get(sessionKey);
            
            if (rawData == null) {
                logger.debug("📭 Session not found: {}", sessionId);
                return Optional.empty();
            }
            
            SessionData sessionData = convertToSessionData(rawData, sessionId);
            if (sessionData == null) {
                return Optional.empty();
            }
            
            DeviceSession session = sessionData.toDeviceSession();
            logger.debug("✅ Session found: {}", sessionId);
            return Optional.of(session);
            
        } catch (Exception e) {
            logger.error("❌ Failed to find session by ID: {}", sessionId, e);
            return Optional.empty();
        }
    }
    
    /**
     * Find session by channel with improved error handling
     */
    public Optional<DeviceSession> findByChannel(Channel channel) {
        if (channel == null) {
            logger.warn("⚠️ Attempt to find session with null channel");
            return Optional.empty();
        }
        
        try {
            String channelId = channel.id().asShortText();
            String channelKey = CHANNEL_SESSION_PREFIX + channelId;
            
            Object sessionIdObj = redisTemplate.opsForValue().get(channelKey);
            if (sessionIdObj == null) {
                logger.debug("📭 No session found for channel: {}", channelId);
                return Optional.empty();
            }
            
            String sessionId = sessionIdObj.toString();
            return findById(sessionId);
            
        } catch (Exception e) {
            logger.error("❌ Failed to find session by channel: {}", channel.id().asShortText(), e);
            return Optional.empty();
        }
    }
    
    /**
     * Find session by IMEI
     */
    public Optional<DeviceSession> findByImei(IMEI imei) {
        try {
            String imeiKey = IMEI_SESSION_PREFIX + imei.getValue();
            Object sessionIdObj = redisTemplate.opsForValue().get(imeiKey);
            
            if (sessionIdObj == null) {
                logger.debug("📭 No session found for IMEI: {}", imei.getValue());
                return Optional.empty();
            }
            
            String sessionId = sessionIdObj.toString();
            return findById(sessionId);
            
        } catch (Exception e) {
            logger.error("❌ Failed to find session by IMEI: {}", imei.getValue(), e);
            return Optional.empty();
        }
    }
    
    /**
     * Delete session with cleanup
     */
    public void delete(String sessionId) {
        try {
            Optional<DeviceSession> sessionOpt = findById(sessionId);
            if (sessionOpt.isPresent()) {
                DeviceSession session = sessionOpt.get();
                
                // Remove main session
                redisTemplate.delete(SESSION_KEY_PREFIX + sessionId);
                
                // Remove channel index
                if (session.getChannelId() != null) {
                    redisTemplate.delete(CHANNEL_SESSION_PREFIX + session.getChannelId());
                }
                
                // Remove IMEI index
                if (session.getImei() != null) {
                    redisTemplate.delete(IMEI_SESSION_PREFIX + session.getImei().getValue());
                }
                
                // Remove from active sessions
                redisTemplate.opsForSet().remove(ACTIVE_SESSIONS_SET, sessionId);
                
                logger.debug("✅ Session deleted successfully: {}", sessionId);
            }
            
        } catch (Exception e) {
            logger.error("❌ Failed to delete session: {}", sessionId, e);
        }
    }
    
    /**
     * Find all sessions with error handling
     */
    public List<DeviceSession> findAll() {
        List<DeviceSession> sessions = new ArrayList<>();
        
        try {
            Set<Object> sessionIds = redisTemplate.opsForSet().members(ACTIVE_SESSIONS_SET);
            if (sessionIds == null || sessionIds.isEmpty()) {
                return sessions;
            }
            
            for (Object sessionIdObj : sessionIds) {
                String sessionId = sessionIdObj.toString();
                Optional<DeviceSession> sessionOpt = findById(sessionId);
                sessionOpt.ifPresent(sessions::add);
            }
            
            logger.debug("✅ Found {} active sessions", sessions.size());
            
        } catch (Exception e) {
            logger.error("❌ Failed to find all sessions", e);
        }
        
        return sessions;
    }
    
    /**
     * Find idle sessions for cleanup
     */
    public List<DeviceSession> findIdleSessions(long idleTimeoutSeconds) {
        List<DeviceSession> idleSessions = new ArrayList<>();
        
        try {
            Instant cutoffTime = Instant.now().minusSeconds(idleTimeoutSeconds);
            List<DeviceSession> allSessions = findAll();
            
            for (DeviceSession session : allSessions) {
                if (session.getLastActivityAt().isBefore(cutoffTime)) {
                    idleSessions.add(session);
                }
            }
            
            logger.debug("✅ Found {} idle sessions", idleSessions.size());
            
        } catch (Exception e) {
            logger.error("❌ Failed to find idle sessions", e);
        }
        
        return idleSessions;
    }
    
    /**
     * Convert raw Redis data to SessionData - handles LinkedHashMap issue
     */
    private SessionData convertToSessionData(Object rawData, String sessionId) {
        try {
            if (rawData instanceof SessionData) {
                return (SessionData) rawData;
            }
            
            if (rawData instanceof LinkedHashMap) {
                // Handle LinkedHashMap from JSON deserialization
                @SuppressWarnings("unchecked")
                Map<String, Object> dataMap = (Map<String, Object>) rawData;
                return convertMapToSessionData(dataMap);
            }
            
            // Try to convert using ObjectMapper
            return redisObjectMapper.convertValue(rawData, SessionData.class);
            
        } catch (Exception e) {
            logger.error("❌ Failed to convert raw data to SessionData for session: {}", sessionId, e);
            // Clean up corrupted session
            redisTemplate.delete(SESSION_KEY_PREFIX + sessionId);
            return null;
        }
    }
    
    /**
     * Convert LinkedHashMap to SessionData
     */
    private SessionData convertMapToSessionData(Map<String, Object> dataMap) {
        SessionData sessionData = new SessionData();
        
        sessionData.setSessionId(getStringValue(dataMap, "sessionId"));
        sessionData.setImei(getStringValue(dataMap, "imei"));
        sessionData.setChannelId(getStringValue(dataMap, "channelId"));
        sessionData.setAuthenticated(getBooleanValue(dataMap, "authenticated"));
        sessionData.setRemoteAddress(getStringValue(dataMap, "remoteAddress"));
        
        // Handle Instant fields
        sessionData.setCreatedAt(getInstantValue(dataMap, "createdAt"));
        sessionData.setLastActivityAt(getInstantValue(dataMap, "lastActivityAt"));
        
        // Handle attributes map
        Object attributesObj = dataMap.get("attributes");
        if (attributesObj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> attributes = (Map<String, Object>) attributesObj;
            sessionData.setAttributes(attributes);
        }
        
        return sessionData;
    }
    
    // Helper methods for type conversion
    private String getStringValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value != null ? value.toString() : null;
    }
    
    private boolean getBooleanValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(value != null ? value.toString() : "false");
    }
    
    private Instant getInstantValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return Instant.now();
        }
        
        try {
            if (value instanceof String) {
                return Instant.parse((String) value);
            }
            if (value instanceof Number) {
                return Instant.ofEpochMilli(((Number) value).longValue());
            }
            // Try to convert using ObjectMapper
            return redisObjectMapper.convertValue(value, Instant.class);
        } catch (Exception e) {
            logger.warn("⚠️ Failed to parse Instant value: {}, using current time", value);
            return Instant.now();
        }
    }
}
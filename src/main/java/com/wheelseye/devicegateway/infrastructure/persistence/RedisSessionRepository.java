package com.wheelseye.devicegateway.infrastructure.persistence;

import com.wheelseye.devicegateway.application.ports.SessionRepository;
import com.wheelseye.devicegateway.domain.entities.DeviceSession;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Repository
public class RedisSessionRepository implements SessionRepository {
    
    private static final Logger logger = LoggerFactory.getLogger(RedisSessionRepository.class);
    private static final String SESSION_PREFIX = "session:";
    private static final String IMEI_INDEX_PREFIX = "imei-index:";
    
    private final RedisTemplate<String, Object> redisTemplate;
    private final Map<Channel, String> channelToSessionId = new ConcurrentHashMap<>();
    
    @Value("${device-gateway.session.idle-timeout}")
    private long sessionTtlSeconds;
    
    public RedisSessionRepository(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }
    
    @Override
    public void save(DeviceSession session) {
        try {
            String sessionKey = SESSION_PREFIX + session.getSessionId();
            String imeiIndexKey = IMEI_INDEX_PREFIX + session.getImei().getValue();
            
            // Store session data
            SessionData sessionData = new SessionData(
                session.getSessionId(),
                session.getImei().getValue(),
                session.getConnectedAt(),
                session.getLastActivity(),
                session.isAuthenticated()
            );
            
            redisTemplate.opsForValue().set(sessionKey, sessionData, sessionTtlSeconds, TimeUnit.SECONDS);
            redisTemplate.opsForValue().set(imeiIndexKey, session.getSessionId(), sessionTtlSeconds, TimeUnit.SECONDS);
            
            // Store channel mapping in memory
            channelToSessionId.put(session.getChannel(), session.getSessionId());
            
            logger.debug("Saved session {} for IMEI: {}", session.getSessionId(), session.getImei().getValue());
            
        } catch (Exception e) {
            logger.error("Failed to save session {}", session.getSessionId(), e);
        }
    }
    
    @Override
    public Optional<DeviceSession> findById(String sessionId) {
        try {
            String sessionKey = SESSION_PREFIX + sessionId;
            SessionData sessionData = (SessionData) redisTemplate.opsForValue().get(sessionKey);
            
            if (sessionData != null) {
                // Find channel by session ID
                Channel channel = findChannelBySessionId(sessionId);
                if (channel != null) {
                    return Optional.of(reconstructSession(sessionData, channel));
                }
            }
            
        } catch (Exception e) {
            logger.error("Failed to find session by ID: {}", sessionId, e);
        }
        
        return Optional.empty();
    }
    
    @Override
    public Optional<DeviceSession> findByImei(IMEI imei) {
        try {
            String imeiIndexKey = IMEI_INDEX_PREFIX + imei.getValue();
            String sessionId = (String) redisTemplate.opsForValue().get(imeiIndexKey);
            
            if (sessionId != null) {
                return findById(sessionId);
            }
            
        } catch (Exception e) {
            logger.error("Failed to find session by IMEI: {}", imei.getValue(), e);
        }
        
        return Optional.empty();
    }
    
    @Override
    public Optional<DeviceSession> findByChannel(Channel channel) {
        String sessionId = channelToSessionId.get(channel);
        if (sessionId != null) {
            return findById(sessionId);
        }
        return Optional.empty();
    }
    
    @Override
    public void deleteById(String sessionId) {
        try {
            Optional<DeviceSession> session = findById(sessionId);
            if (session.isPresent()) {
                String sessionKey = SESSION_PREFIX + sessionId;
                String imeiIndexKey = IMEI_INDEX_PREFIX + session.get().getImei().getValue();
                
                redisTemplate.delete(sessionKey);
                redisTemplate.delete(imeiIndexKey);
                
                channelToSessionId.remove(session.get().getChannel());
                
                logger.debug("Deleted session {}", sessionId);
            }
            
        } catch (Exception e) {
            logger.error("Failed to delete session {}", sessionId, e);
        }
    }
    
    @Override
    public void deleteByChannel(Channel channel) {
        String sessionId = channelToSessionId.get(channel);
        if (sessionId != null) {
            deleteById(sessionId);
        }
    }
    
    @Override
    public Collection<DeviceSession> findAll() {
        List<DeviceSession> sessions = new ArrayList<>();
        try {
            Set<String> sessionKeys = redisTemplate.keys(SESSION_PREFIX + "*");
            if (sessionKeys != null) {
                for (String key : sessionKeys) {
                    String sessionId = key.substring(SESSION_PREFIX.length());
                    findById(sessionId).ifPresent(sessions::add);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to find all sessions", e);
        }
        return sessions;
    }
    
    @Override
    public Collection<DeviceSession> findIdleSessions(long maxIdleSeconds) {
        List<DeviceSession> idleSessions = new ArrayList<>();
        Collection<DeviceSession> allSessions = findAll();
        
        for (DeviceSession session : allSessions) {
            if (session.isIdle(maxIdleSeconds)) {
                idleSessions.add(session);
            }
        }
        
        return idleSessions;
    }
    
    private Channel findChannelBySessionId(String sessionId) {
        return channelToSessionId.entrySet().stream()
            .filter(entry -> entry.getValue().equals(sessionId))
            .map(Map.Entry::getKey)
            .findFirst()
            .orElse(null);
    }
    
    private DeviceSession reconstructSession(SessionData data, Channel channel) {
        DeviceSession session = new DeviceSession(
            data.getSessionId(),
            new IMEI(data.getImei()),
            channel
        );
        
        if (data.isAuthenticated()) {
            session.authenticate();
        }
        
        return session;
    }
    
    // Inner class for Redis storage
    public static class SessionData {
        private String sessionId;
        private String imei;
        private Instant connectedAt;
        private Instant lastActivity;
        private boolean authenticated;
        
        public SessionData() {} // For JSON serialization
        
        public SessionData(String sessionId, String imei, Instant connectedAt, 
                          Instant lastActivity, boolean authenticated) {
            this.sessionId = sessionId;
            this.imei = imei;
            this.connectedAt = connectedAt;
            this.lastActivity = lastActivity;
            this.authenticated = authenticated;
        }
        
        // Getters and setters
        public String getSessionId() { return sessionId; }
        public void setSessionId(String sessionId) { this.sessionId = sessionId; }
        
        public String getImei() { return imei; }
        public void setImei(String imei) { this.imei = imei; }
        
        public Instant getConnectedAt() { return connectedAt; }
        public void setConnectedAt(Instant connectedAt) { this.connectedAt = connectedAt; }
        
        public Instant getLastActivity() { return lastActivity; }
        public void setLastActivity(Instant lastActivity) { this.lastActivity = lastActivity; }
        
        public boolean isAuthenticated() { return authenticated; }
        public void setAuthenticated(boolean authenticated) { this.authenticated = authenticated; }
    }
}

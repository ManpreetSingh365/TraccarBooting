package com.wheelseye.devicegateway.application.services;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.wheelseye.devicegateway.domain.entities.DeviceSession;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;
import com.wheelseye.devicegateway.infrastructure.persistence.RedisSessionRepository;

import io.netty.channel.Channel;

/**
 * Device Session Service - Fixed for proper session management
 * 
 * Key Fixes:
 * 1. ✅ Proper channel ID management (no storing Channel objects)
 * 2. ✅ Enhanced error handling and logging
 * 3. ✅ Better session lifecycle management
 * 4. ✅ Improved cleanup and monitoring
 */
@Service
public class DeviceSessionService {
    
    private static final Logger logger = LoggerFactory.getLogger(DeviceSessionService.class);
    
    @Autowired
    private RedisSessionRepository sessionRepository;
    
    @Value("${device-gateway.session.idle-timeout:600}")
    private int sessionIdleTimeoutSeconds;
    
    @Value("${device-gateway.session.cleanup-interval:60}")
    private int sessionCleanupIntervalSeconds;
    
    /**
     * Create new session with proper channel handling
     */
    public DeviceSession createSession(IMEI imei, Channel channel) {
        try {
            // Check if session already exists for this IMEI
            Optional<DeviceSession> existingSession = sessionRepository.findByImei(imei);
            if (existingSession.isPresent()) {
                DeviceSession existing = existingSession.get();
                logger.info("📱 Reusing existing session for IMEI: {} -> Session: {}", 
                          imei.getValue(), existing.getId());
                
                // Update channel info for existing session
                updateSessionChannel(existing, channel);
                return existing;
            }
            
            // Create new session
            String sessionId = UUID.randomUUID().toString();
            DeviceSession session = new DeviceSession(sessionId, imei);
            
            // Set channel information (store ID, not the Channel object)
            if (channel != null) {
                session.setChannelId(channel.id().asShortText());
                session.setRemoteAddress(channel.remoteAddress() != null ? 
                                       channel.remoteAddress().toString() : "unknown");
            }
            
            // Save to repository
            sessionRepository.save(session);
            
            logger.info("✨ Created new session - IMEI: {} -> Session: {} -> Channel: {}", 
                       imei.getValue(), sessionId, session.getChannelId());
            
            return session;
            
        } catch (Exception e) {
            logger.error("💥 Failed to create session for IMEI: {}", imei.getValue(), e);
            throw new RuntimeException("Failed to create session", e);
        }
    }
    
    /**
     * Get session by channel with improved error handling
     */
    public Optional<DeviceSession> getSession(Channel channel) {
        if (channel == null) {
            logger.warn("⚠️ Attempt to get session with null channel");
            return Optional.empty();
        }
        
        try {
            Optional<DeviceSession> sessionOpt = sessionRepository.findByChannel(channel);
            
            if (sessionOpt.isPresent()) {
                DeviceSession session = sessionOpt.get();
                session.updateActivity();  // Update last activity
                sessionRepository.save(session);  // Persist the update
                
                logger.debug("✅ Found session for channel {}: {}", 
                           channel.id().asShortText(), session.getId());
                return sessionOpt;
            } else {
                logger.debug("📭 No session found for channel: {}", channel.id().asShortText());
                return Optional.empty();
            }
            
        } catch (Exception e) {
            logger.error("💥 Error getting session for channel {}: {}", 
                       channel.id().asShortText(), e.getMessage(), e);
            return Optional.empty();
        }
    }
    
    /**
     * Get session by IMEI
     */
    public Optional<DeviceSession> getSession(IMEI imei) {
        try {
            Optional<DeviceSession> sessionOpt = sessionRepository.findByImei(imei);
            
            if (sessionOpt.isPresent()) {
                DeviceSession session = sessionOpt.get();
                session.updateActivity();
                sessionRepository.save(session);
                
                logger.debug("✅ Found session for IMEI {}: {}", imei.getValue(), session.getId());
                return sessionOpt;
            } else {
                logger.debug("📭 No session found for IMEI: {}", imei.getValue());
                return Optional.empty();
            }
            
        } catch (Exception e) {
            logger.error("💥 Error getting session for IMEI {}: {}", imei.getValue(), e.getMessage(), e);
            return Optional.empty();
        }
    }
    
    /**
     * Get session by ID
     */
    public Optional<DeviceSession> getSession(String sessionId) {
        try {
            return sessionRepository.findById(sessionId);
        } catch (Exception e) {
            logger.error("💥 Error getting session by ID {}: {}", sessionId, e.getMessage(), e);
            return Optional.empty();
        }
    }
    
    /**
     * Remove session with proper cleanup
     */
    public void removeSession(Channel channel) {
        if (channel == null) {
            logger.warn("⚠️ Attempt to remove session with null channel");
            return;
        }
        
        try {
            Optional<DeviceSession> sessionOpt = sessionRepository.findByChannel(channel);
            
            if (sessionOpt.isPresent()) {
                DeviceSession session = sessionOpt.get();
                sessionRepository.delete(session.getId());
                
                logger.info("🗑️ Removed session for channel {}: {} (IMEI: {})", 
                          channel.id().asShortText(), 
                          session.getId(),
                          session.getImei() != null ? session.getImei().getValue() : "unknown");
            } else {
                logger.debug("📭 No session found to remove for channel: {}", 
                           channel.id().asShortText());
            }
            
        } catch (Exception e) {
            logger.error("💥 Error removing session for channel {}: {}", 
                       channel.id().asShortText(), e.getMessage(), e);
        }
    }
    
    /**
     * Remove session by ID
     */
    public void removeSession(String sessionId) {
        try {
            Optional<DeviceSession> sessionOpt = sessionRepository.findById(sessionId);
            if (sessionOpt.isPresent()) {
                DeviceSession session = sessionOpt.get();
                sessionRepository.delete(sessionId);
                
                logger.info("🗑️ Removed session: {} (IMEI: {})", 
                          sessionId, 
                          session.getImei() != null ? session.getImei().getValue() : "unknown");
            }
        } catch (Exception e) {
            logger.error("💥 Error removing session {}: {}", sessionId, e.getMessage(), e);
        }
    }
    
    /**
     * Update session activity
     */
    public void updateActivity(Channel channel) {
        if (channel == null) return;
        
        try {
            Optional<DeviceSession> sessionOpt = getSession(channel);
            if (sessionOpt.isPresent()) {
                DeviceSession session = sessionOpt.get();
                session.updateActivity();
                sessionRepository.save(session);
                
                logger.debug("⏰ Updated activity for session: {}", session.getId());
            }
        } catch (Exception e) {
            logger.error("💥 Error updating activity for channel {}: {}", 
                       channel.id().asShortText(), e.getMessage(), e);
        }
    }
    
    /**
     * Get all active sessions
     */
    public List<DeviceSession> getAllSessions() {
        try {
            List<DeviceSession> sessions = sessionRepository.findAll();
            logger.debug("📊 Retrieved {} active sessions", sessions.size());
            return sessions;
        } catch (Exception e) {
            logger.error("💥 Error getting all sessions: {}", e.getMessage(), e);
            return List.of();
        }
    }
    
    /**
     * Scheduled cleanup of idle sessions
     */
    @Scheduled(fixedRateString = "${device-gateway.session.cleanup-interval:60}000")
    public void cleanupIdleSessions() {
        try {
            List<DeviceSession> idleSessions = sessionRepository.findIdleSessions(sessionIdleTimeoutSeconds);
            
            int cleanedUp = 0;
            for (DeviceSession session : idleSessions) {
                try {
                    sessionRepository.delete(session.getId());
                    cleanedUp++;
                    
                    logger.debug("🧹 Cleaned up idle session: {} (IMEI: {}, idle: {}s)", 
                               session.getId(),
                               session.getImei() != null ? session.getImei().getValue() : "unknown",
                               session.getIdleTimeSeconds());
                               
                } catch (Exception e) {
                    logger.error("💥 Error cleaning up session {}: {}", session.getId(), e.getMessage());
                }
            }
            
            if (cleanedUp > 0) {
                logger.info("🧹 Cleaned up {} idle sessions", cleanedUp);
            }
            
            // Log session statistics
            List<DeviceSession> activeSessions = getAllSessions();
            logger.debug("📊 Session stats - Active: {}, Cleaned: {}", activeSessions.size(), cleanedUp);
            
        } catch (Exception e) {
            logger.error("💥 Error during session cleanup: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Update channel information for existing session
     */
    private void updateSessionChannel(DeviceSession session, Channel channel) {
        try {
            if (channel != null) {
                String newChannelId = channel.id().asShortText();
                String newRemoteAddress = channel.remoteAddress() != null ? 
                                        channel.remoteAddress().toString() : "unknown";
                
                session.setChannelId(newChannelId);
                session.setRemoteAddress(newRemoteAddress);
                session.updateActivity();
                
                sessionRepository.save(session);
                
                logger.debug("🔄 Updated channel info for session {}: {} -> {}", 
                           session.getId(), newChannelId, newRemoteAddress);
            }
        } catch (Exception e) {
            logger.error("💥 Error updating channel for session {}: {}", session.getId(), e.getMessage(), e);
        }
    }
    
    /**
     * Get session statistics
     */
    public SessionStats getSessionStats() {
        try {
            List<DeviceSession> sessions = getAllSessions();
            long authenticatedCount = sessions.stream()
                .mapToLong(s -> s.isAuthenticated() ? 1 : 0)
                .sum();
            
            return new SessionStats(sessions.size(), (int) authenticatedCount, 
                                  sessions.size() - (int) authenticatedCount);
        } catch (Exception e) {
            logger.error("💥 Error getting session stats: {}", e.getMessage(), e);
            return new SessionStats(0, 0, 0);
        }
    }
    
    /**
     * Session statistics record
     */
    public record SessionStats(int totalSessions, int authenticatedSessions, int unauthenticatedSessions) {}
}
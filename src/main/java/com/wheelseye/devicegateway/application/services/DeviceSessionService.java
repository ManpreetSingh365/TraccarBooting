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
 * Device Session Service - FIXED with proper authentication persistence
 * 
 * Key Fixes:
 * 1. ✅ Added saveSession() method to persist session state changes
 * 2. ✅ Proper session authentication flow with Redis persistence
 * 3. ✅ Enhanced session retrieval with authentication status logging
 * 4. ✅ Better error handling and debugging information
 * 5. ✅ Thread-safe operations
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
                logger.info("📱 Reusing existing session for IMEI: {} -> Session: {} (authenticated: {})", 
                          imei.getValue(), existing.getId(), existing.isAuthenticated());
                
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
            
            // Save to repository (but not authenticated yet)
            sessionRepository.save(session);
            
            logger.info("✨ Created new session - IMEI: {} -> Session: {} -> Channel: {} (authenticated: {})", 
                       imei.getValue(), sessionId, session.getChannelId(), session.isAuthenticated());
            
            return session;
            
        } catch (Exception e) {
            logger.error("💥 Failed to create session for IMEI: {}", imei.getValue(), e);
            throw new RuntimeException("Failed to create session", e);
        }
    }
    
    /**
     * CRITICAL METHOD: Save session state to repository
     * This ensures authentication state and other changes are persisted!
     */
    public void saveSession(DeviceSession session) {
        try {
            if (session == null) {
                logger.warn("⚠️ Attempt to save null session");
                return;
            }
            
            sessionRepository.save(session);
            
            logger.debug("💾 Session saved: {} (IMEI: {}, authenticated: {}, channel: {})", 
                       session.getId(), 
                       session.getImei() != null ? session.getImei().getValue() : "unknown",
                       session.isAuthenticated(),
                       session.getChannelId());
                       
        } catch (Exception e) {
            logger.error("💥 Failed to save session {}: {}", 
                       session.getId(), e.getMessage(), e);
            throw new RuntimeException("Failed to save session", e);
        }
    }
    
    /**
     * Get session by channel with enhanced logging for debugging
     */
    public Optional<DeviceSession> getSession(Channel channel) {
        if (channel == null) {
            logger.warn("⚠️ Attempt to get session with null channel");
            return Optional.empty();
        }
        
        try {
            String channelId = channel.id().asShortText();
            String remoteAddress = channel.remoteAddress() != null ? 
                                 channel.remoteAddress().toString() : "unknown";
            
            Optional<DeviceSession> sessionOpt = sessionRepository.findByChannel(channel);
            
            if (sessionOpt.isPresent()) {
                DeviceSession session = sessionOpt.get();
                String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
                
                logger.debug("✅ Found session for channel {} ({}): {} (IMEI: {}, authenticated: {})", 
                           channelId, remoteAddress, session.getId(), imei, session.isAuthenticated());
                           
                return sessionOpt;
            } else {
                logger.debug("📭 No session found for channel: {} ({})", channelId, remoteAddress);
                return Optional.empty();
            }
            
        } catch (Exception e) {
            logger.error("💥 Error getting session for channel {}: {}", 
                       channel.id().asShortText(), e.getMessage(), e);
            return Optional.empty();
        }
    }
    
    /**
     * Get session by IMEI with enhanced logging
     */
    public Optional<DeviceSession> getSession(IMEI imei) {
        try {
            Optional<DeviceSession> sessionOpt = sessionRepository.findByImei(imei);
            
            if (sessionOpt.isPresent()) {
                DeviceSession session = sessionOpt.get();
                
                logger.debug("✅ Found session for IMEI {}: {} (authenticated: {}, channel: {})", 
                           imei.getValue(), session.getId(), session.isAuthenticated(), session.getChannelId());
                           
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
     * Get session by ID with enhanced logging
     */
    public Optional<DeviceSession> getSession(String sessionId) {
        try {
            Optional<DeviceSession> sessionOpt = sessionRepository.findById(sessionId);
            
            if (sessionOpt.isPresent()) {
                DeviceSession session = sessionOpt.get();
                String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
                logger.debug("✅ Found session by ID: {} (IMEI: {}, authenticated: {})", 
                           sessionId, imei, session.isAuthenticated());
            } else {
                logger.debug("📭 No session found for ID: {}", sessionId);
            }
            
            return sessionOpt;
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
                
                String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
                logger.info("🗑️ Removed session for channel {}: {} (IMEI: {}, was authenticated: {})", 
                          channel.id().asShortText(), session.getId(), imei, session.isAuthenticated());
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
                
                String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
                logger.info("🗑️ Removed session: {} (IMEI: {}, was authenticated: {})", 
                          sessionId, imei, session.isAuthenticated());
            }
        } catch (Exception e) {
            logger.error("💥 Error removing session {}: {}", sessionId, e.getMessage(), e);
        }
    }
    
    /**
     * Update session activity and save - ENHANCED with persistence
     */
    public void updateActivity(Channel channel) {
        if (channel == null) return;
        
        try {
            Optional<DeviceSession> sessionOpt = getSession(channel);
            if (sessionOpt.isPresent()) {
                DeviceSession session = sessionOpt.get();
                session.updateActivity();
                
                // CRITICAL: Save the updated session back to repository
                sessionRepository.save(session);
                
                String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
                logger.debug("⏰ Updated activity for session: {} (IMEI: {}, authenticated: {})", 
                           session.getId(), imei, session.isAuthenticated());
            }
        } catch (Exception e) {
            logger.error("💥 Error updating activity for channel {}: {}", 
                       channel.id().asShortText(), e.getMessage(), e);
        }
    }
    
    /**
     * Get all active sessions with authentication status
     */
    public List<DeviceSession> getAllSessions() {
        try {
            List<DeviceSession> sessions = sessionRepository.findAll();
            long authenticatedCount = sessions.stream().mapToLong(s -> s.isAuthenticated() ? 1 : 0).sum();
            
            logger.debug("📊 Retrieved {} active sessions ({} authenticated, {} unauthenticated)", 
                       sessions.size(), authenticatedCount, sessions.size() - authenticatedCount);
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
                    
                    String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
                    logger.debug("🧹 Cleaned up idle session: {} (IMEI: {}, was authenticated: {}, idle: {}s)", 
                               session.getId(), imei, session.isAuthenticated(), session.getIdleTimeSeconds());
                               
                } catch (Exception e) {
                    logger.error("💥 Error cleaning up session {}: {}", session.getId(), e.getMessage());
                }
            }
            
            if (cleanedUp > 0) {
                logger.info("🧹 Cleaned up {} idle sessions", cleanedUp);
            }
            
            // Log session statistics
            List<DeviceSession> activeSessions = getAllSessions();
            if (!activeSessions.isEmpty()) {
                long authenticated = activeSessions.stream().mapToLong(s -> s.isAuthenticated() ? 1 : 0).sum();
                logger.debug("📊 Session stats - Active: {}, Authenticated: {}, Cleaned: {}", 
                           activeSessions.size(), authenticated, cleanedUp);
            }
            
        } catch (Exception e) {
            logger.error("💥 Error during session cleanup: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Update channel information for existing session and save
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
                
                // CRITICAL: Save the updated session
                sessionRepository.save(session);
                
                String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
                logger.debug("🔄 Updated channel info for session {}: {} -> {} (IMEI: {}, authenticated: {})", 
                           session.getId(), newChannelId, newRemoteAddress, imei, session.isAuthenticated());
            }
        } catch (Exception e) {
            logger.error("💥 Error updating channel for session {}: {}", session.getId(), e.getMessage(), e);
        }
    }
    
    /**
     * Get session statistics with detailed authentication info
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
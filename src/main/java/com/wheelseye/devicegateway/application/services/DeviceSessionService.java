package com.wheelseye.devicegateway.application.services;

import com.wheelseye.devicegateway.application.ports.EventPublisher;
import com.wheelseye.devicegateway.application.ports.SessionRepository;
import com.wheelseye.devicegateway.domain.entities.DeviceSession;
import com.wheelseye.devicegateway.domain.events.DeviceSessionEvent;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Optional;
import java.util.UUID;

@Service
public class DeviceSessionService {
    
    private static final Logger logger = LoggerFactory.getLogger(DeviceSessionService.class);
    
    private final SessionRepository sessionRepository;
    private final EventPublisher eventPublisher;
    
    public DeviceSessionService(SessionRepository sessionRepository, EventPublisher eventPublisher) {
        this.sessionRepository = sessionRepository;
        this.eventPublisher = eventPublisher;
    }
    
    public DeviceSession createSession(IMEI imei, Channel channel) {
        // Check if session already exists for this IMEI
        Optional<DeviceSession> existingSession = sessionRepository.findByImei(imei);
        if (existingSession.isPresent()) {
            logger.info("Closing existing session for IMEI: {}", imei.getValue());
            removeSession(existingSession.get().getChannel());
        }
        
        // Create new session
        String sessionId = UUID.randomUUID().toString();
        DeviceSession session = new DeviceSession(sessionId, imei, channel);
        
        sessionRepository.save(session);
        
        // Publish session connected event
        DeviceSessionEvent event = DeviceSessionEvent.connected(
            sessionId, 
            imei, 
            "GT06N", 
            channel.remoteAddress().toString()
        );
        eventPublisher.publishDeviceSessionEvent(event);
        
        logger.info("Created session {} for IMEI: {}", sessionId, imei.getValue());
        return session;
    }
    
    public Optional<DeviceSession> getSession(Channel channel) {
        return sessionRepository.findByChannel(channel);
    }
    
    public Optional<DeviceSession> getSessionByImei(IMEI imei) {
        return sessionRepository.findByImei(imei);
    }
    
    public void removeSession(Channel channel) {
        Optional<DeviceSession> session = sessionRepository.findByChannel(channel);
        if (session.isPresent()) {
            DeviceSession deviceSession = session.get();
            
            sessionRepository.deleteByChannel(channel);
            
            // Publish session disconnected event
            DeviceSessionEvent event = DeviceSessionEvent.disconnected(
                deviceSession.getSessionId(),
                deviceSession.getImei(),
                "GT06N",
                channel.remoteAddress().toString()
            );
            eventPublisher.publishDeviceSessionEvent(event);
            
            logger.info("Removed session {} for IMEI: {}", 
                       deviceSession.getSessionId(), deviceSession.getImei().getValue());
        }
    }
    
    public Collection<DeviceSession> getAllSessions() {
        return sessionRepository.findAll();
    }
    
    @Scheduled(fixedDelay = 60000) // Run every minute
    public void cleanupIdleSessions() {
        Collection<DeviceSession> idleSessions = sessionRepository.findIdleSessions(600); // 10 minutes
        
        for (DeviceSession session : idleSessions) {
            logger.info("Cleaning up idle session {} for IMEI: {}", 
                       session.getSessionId(), session.getImei().getValue());
            
            if (session.getChannel().isActive()) {
                session.getChannel().close();
            }
            
            removeSession(session.getChannel());
        }
        
        if (!idleSessions.isEmpty()) {
            logger.info("Cleaned up {} idle sessions", idleSessions.size());
        }
    }
}

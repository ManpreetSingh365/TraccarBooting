package com.wheelseye.devicegateway.infrastructure.netty;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.netty.channel.Channel;

/**
 * Default implementation of ChannelRegistry
 * 
 * Thread-safe registry for managing active Netty channels.
 * Automatically cleans up inactive channels periodically.
 */
@Component
public class DefaultChannelRegistry implements ChannelRegistry {
    
    private static final Logger logger = LoggerFactory.getLogger(DefaultChannelRegistry.class);
    
    private final ConcurrentMap<String, Channel> channels = new ConcurrentHashMap<>();
    private final AtomicInteger registrationCounter = new AtomicInteger(0);
    
    @Override
    public void register(String channelId, Channel channel) {
        if (channelId == null || channel == null) {
            logger.warn("⚠️ Attempt to register null channel or channelId");
            return;
        }
        
        try {
            Channel previousChannel = channels.put(channelId, channel);
            
            if (previousChannel != null) {
                logger.debug("🔄 Replaced existing channel registration: {}", channelId);
            } else {
                registrationCounter.incrementAndGet();
                logger.debug("📝 Registered new channel: {} (Total: {})", 
                           channelId, channels.size());
            }
            
        } catch (Exception e) {
            logger.error("💥 Error registering channel {}: {}", channelId, e.getMessage(), e);
        }
    }
    
    @Override
    public Channel get(String channelId) {
        if (channelId == null) {
            return null;
        }
        
        try {
            Channel channel = channels.get(channelId);
            
            if (channel != null && !channel.isActive()) {
                // Channel is no longer active, remove it
                channels.remove(channelId);
                logger.debug("🧹 Removed inactive channel: {}", channelId);
                return null;
            }
            
            return channel;
            
        } catch (Exception e) {
            logger.error("💥 Error getting channel {}: {}", channelId, e.getMessage(), e);
            return null;
        }
    }
    
    @Override
    public void unregister(String channelId) {
        if (channelId == null) {
            return;
        }
        
        try {
            Channel removed = channels.remove(channelId);
            
            if (removed != null) {
                logger.debug("🗑️ Unregistered channel: {} (Remaining: {})", 
                           channelId, channels.size());
            } else {
                logger.debug("📭 Channel not found for unregistration: {}", channelId);
            }
            
        } catch (Exception e) {
            logger.error("💥 Error unregistering channel {}: {}", channelId, e.getMessage(), e);
        }
    }
    
    @Override
    public int getActiveChannelCount() {
        return channels.size();
    }
    
    @Override
    public boolean isChannelActive(String channelId) {
        if (channelId == null) {
            return false;
        }
        
        Channel channel = channels.get(channelId);
        return channel != null && channel.isActive();
    }
    
    @Override
    @Scheduled(fixedRate = 60000) // Run every minute
    public int cleanupInactiveChannels() {
        try {
            int cleanedUp = 0;
            
            // Use entrySet to avoid ConcurrentModificationException
            var iterator = channels.entrySet().iterator();
            
            while (iterator.hasNext()) {
                var entry = iterator.next();
                String channelId = entry.getKey();
                Channel channel = entry.getValue();
                
                if (channel == null || !channel.isActive()) {
                    iterator.remove();
                    cleanedUp++;
                    logger.debug("🧹 Cleaned up inactive channel: {}", channelId);
                }
            }
            
            if (cleanedUp > 0) {
                logger.info("🧹 Cleaned up {} inactive channels (Remaining: {})", 
                          cleanedUp, channels.size());
            }
            
            // Log statistics periodically
            if (registrationCounter.get() % 100 == 0) {
                logger.info("📊 Channel Registry Stats - Active: {}, Total Registered: {}", 
                          channels.size(), registrationCounter.get());
            }
            
            return cleanedUp;
            
        } catch (Exception e) {
            logger.error("💥 Error during channel cleanup: {}", e.getMessage(), e);
            return 0;
        }
    }
    
    /**
     * Get registry statistics
     */
    public ChannelStats getStats() {
        return new ChannelStats(
            channels.size(),
            registrationCounter.get(),
            (int) channels.values().stream().mapToLong(c -> c.isActive() ? 1 : 0).sum()
        );
    }
    
    /**
     * Registry statistics record
     */
    public record ChannelStats(int activeChannels, int totalRegistered, int confirmedActive) {}
    
    /**
     * Get all channel IDs (for debugging)
     */
    public java.util.Set<String> getChannelIds() {
        return java.util.Set.copyOf(channels.keySet());
    }
}
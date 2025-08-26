package com.wheelseye.devicegateway.infrastructure.netty;

import io.netty.channel.Channel;

/**
 * Channel Registry Interface for managing active channels
 * 
 * Provides a way to store and retrieve Netty channels by their ID
 * so that commands can be sent to specific connected devices.
 */
public interface ChannelRegistry {
    
    /**
     * Register a channel with its ID
     * 
     * @param channelId the channel ID (usually channel.id().asShortText())
     * @param channel the Netty channel
     */
    void register(String channelId, Channel channel);
    
    /**
     * Get a channel by its ID
     * 
     * @param channelId the channel ID
     * @return the channel if found and active, null otherwise
     */
    Channel get(String channelId);
    
    /**
     * Unregister a channel
     * 
     * @param channelId the channel ID to remove
     */
    void unregister(String channelId);
    
    /**
     * Get the number of active channels
     * 
     * @return count of active channels
     */
    int getActiveChannelCount();
    
    /**
     * Check if a channel is registered and active
     * 
     * @param channelId the channel ID
     * @return true if channel exists and is active
     */
    boolean isChannelActive(String channelId);
    
    /**
     * Clean up inactive channels
     * Removes channels that are no longer active
     * 
     * @return number of cleaned up channels
     */
    int cleanupInactiveChannels();
}
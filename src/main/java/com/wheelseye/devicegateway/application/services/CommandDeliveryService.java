package com.wheelseye.devicegateway.application.services;

import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wheelseye.devicegateway.domain.entities.DeviceSession;
import com.wheelseye.devicegateway.domain.events.CommandEvent;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;
import com.wheelseye.devicegateway.infrastructure.netty.ChannelRegistry;
import com.wheelseye.devicegateway.infrastructure.netty.GT06ProtocolParser;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

/**
 * Command Delivery Service - Fixed version with proper dependencies
 * 
 * Key Fixes:
 * 1. ✅ Uses DeviceSessionService instead of SessionRepository
 * 2. ✅ Proper ChannelRegistry injection and usage
 * 3. ✅ Enhanced error handling and validation
 * 4. ✅ Better command frame building
 * 5. ✅ Improved logging with emojis
 */
@Service
public class CommandDeliveryService {
    
    private static final Logger logger = LoggerFactory.getLogger(CommandDeliveryService.class);
    
    @Autowired
    private DeviceSessionService sessionService;
    
    @Autowired
    private GT06ProtocolParser protocolParser;
    
    @Autowired
    private ChannelRegistry channelRegistry;
    
    private static int serialCounter = 1;
    
    /**
     * Deliver command to device with comprehensive validation
     */
    public void deliverCommand(CommandEvent command) {
        if (command == null || command.getImei() == null) {
            logger.warn("❌ Invalid command event received");
            return;
        }
        
        IMEI imei = command.getImei();
        String commandType = command.getCommand();
        
        logger.info("📤 Delivering command '{}' to IMEI: {}", commandType, imei.getValue());
        
        try {
            // Step 1: Get active session for the IMEI
            Optional<DeviceSession> sessionOpt = sessionService.getSession(imei);
            
            if (!sessionOpt.isPresent()) {
                logger.warn("❌ No active session found for IMEI: {} to deliver command: {}", 
                           imei.getValue(), commandType);
                return;
            }
            
            DeviceSession session = sessionOpt.get();
            
            // Step 2: Validate session is authenticated
            if (!session.isAuthenticated()) {
                logger.warn("❌ Session not authenticated for IMEI: {} to deliver command: {}", 
                           imei.getValue(), commandType);
                return;
            }
            
            // Step 3: Get channel from registry using session's channel ID
            String channelId = session.getChannelId();
            if (channelId == null || channelId.isEmpty()) {
                logger.warn("❌ No channel ID found for IMEI: {} to deliver command: {}", 
                           imei.getValue(), commandType);
                return;
            }
            
            Channel channel = channelRegistry.get(channelId);
            if (channel == null || !channel.isActive()) {
                logger.warn("❌ Channel not active for IMEI: {} (Channel ID: {}) to deliver command: {}", 
                           imei.getValue(), channelId, commandType);
                return;
            }
            
            // Step 4: Build command frame
            ByteBuf commandFrame = buildCommandFrame(command);
            if (commandFrame == null) {
                logger.error("❌ Failed to build command frame for command: {} to IMEI: {}", 
                           commandType, imei.getValue());
                return;
            }
            
            // Step 5: Send command with callback
            channel.writeAndFlush(commandFrame).addListener(future -> {
                if (future.isSuccess()) {
                    logger.info("✅ Successfully delivered command '{}' to IMEI: {}", 
                               commandType, imei.getValue());
                    
                    // Update session activity
                    sessionService.updateActivity(channel);
                    
                } else {
                    String errorMsg = future.cause() != null ? future.cause().getMessage() : "Unknown error";
                    logger.error("❌ Failed to deliver command '{}' to IMEI: {}: {}", 
                               commandType, imei.getValue(), errorMsg);
                }
            });
            
        } catch (Exception e) {
            logger.error("💥 Error delivering command '{}' to IMEI: {}: {}", 
                       commandType, imei.getValue(), e.getMessage(), e);
        }
    }
    
    /**
     * Build command frame based on command type
     */
    private ByteBuf buildCommandFrame(CommandEvent command) {
        if (command == null || command.getCommand() == null) {
            return null;
        }
        
        try {
            String commandType = command.getCommand().toUpperCase();
            
            return switch (commandType) {
                case "IMMOBILIZE" -> buildImmobilizeCommand(command);
                case "SIREN" -> buildSirenCommand(command);
                case "LOCATE" -> buildLocateCommand(command);
                case "ENGINE_ON" -> buildEngineCommand(true);
                case "ENGINE_OFF" -> buildEngineCommand(false);
                case "RESTART" -> buildRestartCommand();
                case "RESET" -> buildResetCommand();
                default -> buildGenericCommand(command);
            };
            
        } catch (Exception e) {
            logger.error("💥 Error building command frame for '{}': {}", 
                       command.getCommand(), e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Build immobilizer command
     */
    private ByteBuf buildImmobilizeCommand(CommandEvent command) {
        try {
            ByteBuf frame = Unpooled.buffer(20);
            
            // Start bits
            frame.writeShort(0x7878);
            
            // Length placeholder
            int lengthIndex = frame.writerIndex();
            frame.writeByte(0x00);
            
            // Protocol number for command
            frame.writeByte(0x80);
            
            // Command content
            Map<String, Object> params = command.getParameters();
            String action = params != null ? (String) params.getOrDefault("action", "enable") : "enable";
            String commandStr = "enable".equals(action) ? "DYD#" : "HFYD#";
            frame.writeBytes(commandStr.getBytes());
            
            // Serial number
            frame.writeShort(getNextSerialNumber());
            
            // Update length
            int contentLength = frame.writerIndex() - lengthIndex - 1;
            frame.setByte(lengthIndex, contentLength + 2); // +2 for CRC
            
            // Calculate and write CRC
            int crc = calculateCrc(frame, 2, contentLength + 1);
            frame.writeShort(crc);
            
            // Stop bits
            frame.writeShort(0x0D0A);
            
            logger.debug("🔧 Built immobilize command: {} (action: {})", commandStr, action);
            return frame;
            
        } catch (Exception e) {
            logger.error("💥 Error building immobilize command: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Build siren command
     */
    private ByteBuf buildSirenCommand(CommandEvent command) {
        try {
            ByteBuf frame = Unpooled.buffer(20);
            
            // Start bits
            frame.writeShort(0x7878);
            
            // Length placeholder
            int lengthIndex = frame.writerIndex();
            frame.writeByte(0x00);
            
            // Protocol number
            frame.writeByte(0x80);
            
            // Siren command
            Map<String, Object> params = command.getParameters();
            boolean enable = params != null ? (Boolean) params.getOrDefault("enable", true) : true;
            String sirenCmd = enable ? "DXDY#" : "QXDY#";
            frame.writeBytes(sirenCmd.getBytes());
            
            // Serial number
            frame.writeShort(getNextSerialNumber());
            
            // Update length
            int contentLength = frame.writerIndex() - lengthIndex - 1;
            frame.setByte(lengthIndex, contentLength + 2);
            
            // CRC
            int crc = calculateCrc(frame, 2, contentLength + 1);
            frame.writeShort(crc);
            
            // Stop bits
            frame.writeShort(0x0D0A);
            
            logger.debug("🔔 Built siren command: {} (enable: {})", sirenCmd, enable);
            return frame;
            
        } catch (Exception e) {
            logger.error("💥 Error building siren command: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Build locate command
     */
    private ByteBuf buildLocateCommand(CommandEvent command) {
        try {
            ByteBuf frame = Unpooled.buffer(15);
            
            // Start bits
            frame.writeShort(0x7878);
            
            // Length
            frame.writeByte(0x05);
            
            // Protocol number for location request
            frame.writeByte(0x8A);
            
            // Serial number
            frame.writeShort(getNextSerialNumber());
            
            // CRC
            int crc = calculateCrc(frame, 2, 3);
            frame.writeShort(crc);
            
            // Stop bits
            frame.writeShort(0x0D0A);
            
            logger.debug("📍 Built locate command");
            return frame;
            
        } catch (Exception e) {
            logger.error("💥 Error building locate command: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Build engine control command
     */
    private ByteBuf buildEngineCommand(boolean enable) {
        try {
            ByteBuf frame = Unpooled.buffer(20);
            
            // Start bits
            frame.writeShort(0x7878);
            
            // Length placeholder
            int lengthIndex = frame.writerIndex();
            frame.writeByte(0x00);
            
            // Protocol number
            frame.writeByte(0x80);
            
            // Engine command
            String engineCmd = enable ? "RELAY,1#" : "RELAY,0#";
            frame.writeBytes(engineCmd.getBytes());
            
            // Serial number
            frame.writeShort(getNextSerialNumber());
            
            // Update length
            int contentLength = frame.writerIndex() - lengthIndex - 1;
            frame.setByte(lengthIndex, contentLength + 2);
            
            // CRC
            int crc = calculateCrc(frame, 2, contentLength + 1);
            frame.writeShort(crc);
            
            // Stop bits
            frame.writeShort(0x0D0A);
            
            logger.debug("🚗 Built engine command: {} (enable: {})", engineCmd, enable);
            return frame;
            
        } catch (Exception e) {
            logger.error("💥 Error building engine command: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Build restart command
     */
    private ByteBuf buildRestartCommand() {
        return buildSimpleCommand("RESET#");
    }
    
    /**
     * Build reset command  
     */
    private ByteBuf buildResetCommand() {
        return buildSimpleCommand("FACTORY#");
    }
    
    /**
     * Build simple command with no parameters
     */
    private ByteBuf buildSimpleCommand(String cmdStr) {
        try {
            ByteBuf frame = Unpooled.buffer(20);
            
            // Start bits
            frame.writeShort(0x7878);
            
            // Length placeholder
            int lengthIndex = frame.writerIndex();
            frame.writeByte(0x00);
            
            // Protocol number
            frame.writeByte(0x80);
            
            // Command string
            frame.writeBytes(cmdStr.getBytes());
            
            // Serial number
            frame.writeShort(getNextSerialNumber());
            
            // Update length
            int contentLength = frame.writerIndex() - lengthIndex - 1;
            frame.setByte(lengthIndex, contentLength + 2);
            
            // CRC
            int crc = calculateCrc(frame, 2, contentLength + 1);
            frame.writeShort(crc);
            
            // Stop bits
            frame.writeShort(0x0D0A);
            
            logger.debug("🔧 Built simple command: {}", cmdStr);
            return frame;
            
        } catch (Exception e) {
            logger.error("💥 Error building simple command '{}': {}", cmdStr, e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Build generic command
     */
    private ByteBuf buildGenericCommand(CommandEvent command) {
        try {
            ByteBuf frame = Unpooled.buffer(50);
            
            // Start bits
            frame.writeShort(0x7878);
            
            // Length placeholder
            int lengthIndex = frame.writerIndex();
            frame.writeByte(0x00);
            
            // Protocol number
            frame.writeByte(0x80);
            
            // Command string
            String cmdStr = command.getCommand() + "#";
            frame.writeBytes(cmdStr.getBytes());
            
            // Serial number
            frame.writeShort(getNextSerialNumber());
            
            // Update length
            int contentLength = frame.writerIndex() - lengthIndex - 1;
            frame.setByte(lengthIndex, contentLength + 2);
            
            // CRC
            int crc = calculateCrc(frame, 2, contentLength + 1);
            frame.writeShort(crc);
            
            // Stop bits
            frame.writeShort(0x0D0A);
            
            logger.debug("🔧 Built generic command: {}", cmdStr);
            return frame;
            
        } catch (Exception e) {
            logger.error("💥 Error building generic command: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Calculate CRC for GT06 protocol
     */
    private int calculateCrc(ByteBuf buffer, int start, int length) {
        int crc = 0xFFFF;
        
        for (int i = start; i < start + length; i++) {
            crc ^= (buffer.getByte(i) & 0xFF) << 8;
            for (int j = 0; j < 8; j++) {
                if ((crc & 0x8000) != 0) {
                    crc = (crc << 1) ^ 0x1021;
                } else {
                    crc <<= 1;
                }
                crc &= 0xFFFF;
            }
        }
        
        return crc;
    }
    
    /**
     * Get next serial number (thread-safe)
     */
    private synchronized int getNextSerialNumber() {
        serialCounter = (serialCounter % 0xFFFF) + 1;
        return serialCounter;
    }
    
    /**
     * Check if device is available for commands
     */
    public boolean isDeviceAvailable(IMEI imei) {
        try {
            Optional<DeviceSession> sessionOpt = sessionService.getSession(imei);
            
            if (!sessionOpt.isPresent()) {
                return false;
            }
            
            DeviceSession session = sessionOpt.get();
            
            if (!session.isAuthenticated()) {
                return false;
            }
            
            String channelId = session.getChannelId();
            return channelId != null && channelRegistry.isChannelActive(channelId);
            
        } catch (Exception e) {
            logger.error("💥 Error checking device availability for IMEI {}: {}", 
                       imei.getValue(), e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * Get command delivery statistics
     */
    public CommandStats getCommandStats() {
        try {
            var sessionStats = sessionService.getSessionStats();
            int activeChannels = channelRegistry.getActiveChannelCount();
            
            return new CommandStats(
                sessionStats.totalSessions(),
                sessionStats.authenticatedSessions(),
                activeChannels,
                serialCounter - 1
            );
        } catch (Exception e) {
            logger.error("💥 Error getting command stats: {}", e.getMessage(), e);
            return new CommandStats(0, 0, 0, 0);
        }
    }
    
    /**
     * Command statistics record
     */
    public record CommandStats(int totalSessions, int authenticatedSessions, int activeChannels, int commandsSent) {}
}
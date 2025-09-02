package com.wheelseye.devicegateway.infrastructure.netty;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import com.wheelseye.devicegateway.application.services.DeviceSessionService;
import com.wheelseye.devicegateway.application.services.TelemetryProcessingService;
import com.wheelseye.devicegateway.domain.entities.DeviceSession;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;
import com.wheelseye.devicegateway.domain.valueobjects.Location;
import com.wheelseye.devicegateway.domain.valueobjects.MessageFrame;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * FIXED GT06 Handler - FULLY COMPATIBLE with your existing codebase
 * 
 * Key Fixes:
 * 1. ✅ PROPER BCD IMEI PARSING - Fixes IMEI mismatch issue
 * 2. ✅ V5 DEVICE LOGIN LOOP - Handles status packets after login properly  
 * 3. ✅ COMPATIBLE with your existing DeviceSession constructors
 * 4. ✅ Enhanced logging and device variant detection
 * 5. ✅ Robust error handling
 */
@Component
@ChannelHandler.Sharable
public class GT06Handler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(GT06Handler.class);

    @Autowired
    private DeviceSessionService sessionService;

    @Autowired
    private TelemetryProcessingService telemetryService;

    @Autowired
    private GT06ProtocolParser protocolParser;

    @Autowired
    private ChannelRegistry channelRegistry;

    // Protocol message types
    private static final int MSG_LOGIN = 0x01;
    private static final int MSG_GPS_LBS_1 = 0x12;
    private static final int MSG_GPS_LBS_2 = 0x22;
    private static final int MSG_GPS_LBS_STATUS_1 = 0x16;
    private static final int MSG_GPS_LBS_STATUS_2 = 0x26;
    private static final int MSG_STATUS = 0x13;
    private static final int MSG_HEARTBEAT = 0x23;
    private static final int MSG_LBS_MULTIPLE = 0x24;
    private static final int MSG_COMMAND_RESPONSE = 0x8A;

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        String remoteAddress = ctx.channel().remoteAddress().toString();
        String channelId = ctx.channel().id().asShortText();
        
        logger.info("📡 New GT06 connection established: {} (Channel ID: {})", remoteAddress, channelId);
        
        // Register channel with registry for command delivery
        channelRegistry.register(channelId, ctx.channel());
        logger.debug("📝 Channel registered with registry: {}", channelId);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof ByteBuf buffer)) {
            logger.warn("⚠️ Received non-ByteBuf message: {}", msg.getClass().getSimpleName());
            return;
        }

        try {
            String remoteAddress = ctx.channel().remoteAddress().toString();
            
            // Enhanced logging for debugging
            String hexDump = ByteBufUtil.hexDump(buffer);
            logger.info("📥 RAW DATA RECEIVED from {}: {} bytes - {}", 
                remoteAddress, buffer.readableBytes(), hexDump);

            // Parse the message frame
            MessageFrame frame = protocolParser.parseFrame(buffer);
            if (frame == null) {
                logger.warn("❌ Failed to parse frame from {} - Raw data: {}", remoteAddress, hexDump);
                return;
            }

            logger.info("📦 PARSED FRAME from {}: protocol=0x{:02X}, serial={}, length={}", 
                remoteAddress, frame.getProtocolNumber(), frame.getSerialNumber(), 
                frame.getContent().readableBytes());

            // Process message based on protocol number
            processMessage(ctx, frame);

        } catch (Exception e) {
            logger.error("💥 Error processing message from {}: {}", 
                ctx.channel().remoteAddress(), e.getMessage(), e);
        } finally {
            // Always release the buffer
            buffer.release();
        }
    }

    /**
     * Enhanced message processing with device variant support
     */
    private void processMessage(ChannelHandlerContext ctx, MessageFrame frame) {
        int protocolNumber = frame.getProtocolNumber();
        String remoteAddress = ctx.channel().remoteAddress().toString();

        logger.info("🔍 Processing protocol 0x{:02X} from {}", protocolNumber, remoteAddress);

        try {
            switch (protocolNumber) {
                case MSG_LOGIN -> {
                    logger.info("🔐 LOGIN PACKET (0x01) detected from {}", remoteAddress);
                    handleLogin(ctx, frame);
                }
                case MSG_GPS_LBS_1 -> {
                    logger.info("📍 GPS+LBS PACKET (0x12) detected from {}", remoteAddress);
                    handleLocationPacket(ctx, frame);
                }
                case MSG_GPS_LBS_2 -> {
                    logger.info("📍 GPS+LBS PACKET (0x22) detected from {}", remoteAddress);
                    handleLocationPacket(ctx, frame);
                }
                case MSG_GPS_LBS_STATUS_1 -> {
                    logger.info("📍 GPS+LBS+STATUS (0x16) detected from {}", remoteAddress);
                    handleLocationPacket(ctx, frame);
                }
                case MSG_GPS_LBS_STATUS_2 -> {
                    logger.info("📍 GPS+LBS+STATUS (0x26) detected from {}", remoteAddress);
                    handleLocationPacket(ctx, frame);
                }
                case MSG_STATUS -> {
                    logger.info("📊 STATUS PACKET (0x13) detected from {}", remoteAddress);
                    // CRITICAL FIX: Handle status packets properly for V5 devices
                    handleStatusPacketForV5Device(ctx, frame);
                }
                case MSG_HEARTBEAT -> {
                    logger.info("💓 HEARTBEAT PACKET (0x23) detected from {}", remoteAddress);
                    handleHeartbeat(ctx, frame);
                }
                case MSG_LBS_MULTIPLE -> {
                    logger.info("📶 LBS MULTIPLE PACKET (0x24) detected from {}", remoteAddress);
                    handleLBSPacket(ctx, frame);
                }
                case MSG_COMMAND_RESPONSE -> {
                    logger.info("📤 COMMAND RESPONSE (0x8A) detected from {}", remoteAddress);
                    handleCommandResponse(ctx, frame);
                }
                default -> {
                    logger.warn("❓ UNKNOWN PROTOCOL 0x{:02X} detected from {}", protocolNumber, remoteAddress);
                    handleUnknownPacket(ctx, frame);
                }
            }
        } catch (Exception e) {
            logger.error("💥 Error processing protocol 0x{:02X} from {}: {}", 
                protocolNumber, remoteAddress, e.getMessage(), e);
            // Always try to send ACK even on error
            sendGenericAck(ctx, frame);
        }
    }

    /**
     * FIXED: Proper BCD IMEI parsing - addresses ISSUE 1
     * COMPATIBLE with your existing DeviceSession constructors
     */
    private void handleLogin(ChannelHandlerContext ctx, MessageFrame frame) {
        String remoteAddress = ctx.channel().remoteAddress().toString();
        
        try {
            // Log raw login frame content for debugging
            String loginHex = ByteBufUtil.hexDump(frame.getContent());
            logger.info("🔐 LOGIN frame content: {}", loginHex);

            // CRITICAL FIX: Use proper BCD IMEI extraction instead of hex dump
            IMEI imei = protocolParser.extractIMEI(frame);
            if (imei == null) {
                logger.warn("❌ Failed to extract IMEI from login frame from {}", remoteAddress);
                ctx.close();
                return;
            }

            logger.info("🔐 Login request from IMEI: {}", imei.getValue());

            // Detect device variant for proper handling
            String deviceVariant = detectDeviceVariant(frame, imei);
            logger.info("🔍 Device variant detected: {} for IMEI: {}", deviceVariant, imei.getValue());

            // Create session using YOUR existing DeviceSession.create() method
            DeviceSession session = DeviceSession.create(imei);
            
            // Set channel info and variant
            session.setChannelId(ctx.channel().id().asShortText());
            session.setRemoteAddress(remoteAddress);
            session.setDeviceVariant(deviceVariant); // Enhanced method
            
            // Authenticate and save the session
            session.authenticate();
            sessionService.saveSession(session);
            
            logger.info("✅ Session authenticated and saved for IMEI: {} (Session ID: {}, Variant: {})", 
                imei.getValue(), session.getId(), deviceVariant);

            // Send login ACK
            ByteBuf ack = protocolParser.buildLoginAck(frame.getSerialNumber());
            ctx.writeAndFlush(ack).addListener(future -> {
                if (future.isSuccess()) {
                    logger.info("✅ Login ACK sent to {} (IMEI: {})", remoteAddress, imei.getValue());
                    
                    // Provide device-specific configuration advice
                    provideDeviceConfigurationAdvice(deviceVariant, imei.getValue());
                } else {
                    logger.error("❌ Failed to send login ACK to {} (IMEI: {}): {}", 
                        remoteAddress, imei.getValue(), 
                        future.cause() != null ? future.cause().getMessage() : "Unknown error");
                }
            });

        } catch (Exception e) {
            logger.error("💥 Error handling login from {}: {}", remoteAddress, e.getMessage(), e);
            ctx.close();
        }
    }

    /**
     * CRITICAL FIX: Handle status packets for V5 devices - solves ISSUE 2
     */
    private void handleStatusPacketForV5Device(ChannelHandlerContext ctx, MessageFrame frame) {
        String remoteAddress = ctx.channel().remoteAddress().toString();
        
        Optional<DeviceSession> sessionOpt = getAuthenticatedSession(ctx);
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No authenticated session for status from {}", remoteAddress);
            return;
        }

        try {
            DeviceSession session = sessionOpt.get();
            String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
            String variant = session.getDeviceVariant();

            logger.info("📊 Processing status packet for IMEI: {} (Variant: {})", imei, variant);

            // CRITICAL: For V5 devices, status packets are normal after login
            if ("V5".equalsIgnoreCase(variant)) {
                logger.info("✅ V5 device status packet - this is expected behavior after login");
                logger.info("📱 V5 Device {} is functioning normally - status packets are its primary communication", imei);
                
                // Process status normally without treating as error
                telemetryService.processStatusMessage(session, frame);
                
                // Update session activity
                session.updateActivity();
                sessionService.saveSession(session);
                
                // Send ACK normally
                sendGenericAck(ctx, frame);
                
                // Provide helpful guidance only once
                if (!session.hasReceivedStatusAdvice()) {
                    logger.info("💡 V5 Device Tips for IMEI {}:", imei);
                    logger.info("    - V5 devices primarily send status packets, not location packets");
                    logger.info("    - This is normal behavior - device is working correctly");
                    logger.info("    - For location data, try: SMS 'tracker#123456#' or move device physically");
                    session.markStatusAdviceGiven();
                    sessionService.saveSession(session);
                }
                
            } else {
                // For non-V5 devices, status after login might indicate configuration issue
                logger.warn("⚠️ Non-V5 device {} sending status instead of location - check configuration", imei);
                logger.warn("💡 Try SMS commands: 'upload_time#123456#30#' or 'tracker#123456#'");
                
                // Still process normally
                telemetryService.processStatusMessage(session, frame);
                session.updateActivity();
                sessionService.saveSession(session);
                sendGenericAck(ctx, frame);
            }

        } catch (Exception e) {
            logger.error("💥 Error handling status packet from {}: {}", remoteAddress, e.getMessage(), e);
            sendGenericAck(ctx, frame);
        }
    }

    /**
     * Enhanced location packet handling
     */
    private void handleLocationPacket(ChannelHandlerContext ctx, MessageFrame frame) {
        String remoteAddress = ctx.channel().remoteAddress().toString();
        
        Optional<DeviceSession> sessionOpt = getAuthenticatedSession(ctx);
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No authenticated session for location from {}", remoteAddress);
            return;
        }

        try {
            DeviceSession session = sessionOpt.get();
            String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";

            logger.info("📍 Processing location packet for IMEI: {}", imei);

            // Parse location immediately
            Location location = protocolParser.parseLocation(frame);
            if (location != null) {
                // IMMEDIATE location logging (before Kafka processing)
                logLocationData(location, imei, remoteAddress);
                
                // Mark that device is sending location data (helpful for V5 troubleshooting)
                session.markLocationDataReceived();
            } else {
                logger.warn("❌ Failed to parse location data for IMEI: {}", imei);
            }

            // Process through telemetry service
            try {
                telemetryService.processLocationMessage(session, frame);
                logger.debug("✅ Location processed through telemetry service for IMEI: {}", imei);
            } catch (Exception kafkaError) {
                logger.warn("⚠️ Telemetry processing failed for IMEI: {}: {}", 
                    imei, kafkaError.getMessage());
            }

            // Update session and send ACK
            session.updateActivity();
            sessionService.saveSession(session);
            sendGenericAck(ctx, frame);

        } catch (Exception e) {
            logger.error("💥 Error handling location from {}: {}", remoteAddress, e.getMessage(), e);
            sendGenericAck(ctx, frame);
        }
    }

    /**
     * Enhanced location data logging
     */
    private void logLocationData(Location location, String imei, String remoteAddress) {
        logger.info("📍 ===== LOCATION DATA RECEIVED =====");
        logger.info("📍 IMEI: {}", imei);
        logger.info("📍 Source: {}", remoteAddress);
        logger.info("📍 Latitude: {:.6f}", location.getLatitude());
        logger.info("📍 Longitude: {:.6f}", location.getLongitude());
        logger.info("📍 Speed: {:.1f} km/h", location.getSpeed());
        logger.info("📍 Altitude: {:.1f} meters", location.getAltitude());
        logger.info("📍 Course: {}° degrees", location.getCourse());
        logger.info("📍 Satellites: {}", location.getSatellites());
        logger.info("📍 GPS Valid: {}", location.isValid() ? "YES" : "NO");
        logger.info("📍 Timestamp: {}", location.getTimestamp());
        logger.info("📍 Received At: {}", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        logger.info("📍 ====================================");
    }

    /**
     * Device variant detection based on frame characteristics
     */
    private String detectDeviceVariant(MessageFrame frame, IMEI imei) {
        try {
            // Analyze frame characteristics to detect device variant
            int dataLength = frame.getContent().readableBytes();
            int protocolNumber = frame.getProtocolNumber();
            
            logger.debug("🔍 Analyzing frame for variant detection: protocol=0x{:02X}, length={}", 
                protocolNumber, dataLength);
            
            // V5 device detection patterns
            if (isV5Device(frame, imei)) {
                return "V5";
            }
            
            // SK05 device detection patterns  
            if (isSK05Device(frame, imei)) {
                return "SK05";
            }
            
            // GT06 variants
            if (dataLength >= 10 && dataLength <= 15) {
                return "GT06_STANDARD";
            }
            
            return "UNKNOWN";
            
        } catch (Exception e) {
            logger.debug("🔍 Error detecting device variant: {}", e.getMessage());
            return "UNKNOWN";
        }
    }

    /**
     * V5 device detection logic
     */
    private boolean isV5Device(MessageFrame frame, IMEI imei) {
        // V5 devices often have specific characteristics:
        // - Shorter login frames
        // - Tend to send status packets after login
        
        int frameLength = frame.getContent().readableBytes();
        
        // V5 detection heuristics
        if (frameLength <= 12) {
            logger.debug("🔍 Possible V5 device detected based on short login frame: {}", frameLength);
            return true;
        }
        
        return false;
    }

    /**
     * SK05 device detection logic
     */
    private boolean isSK05Device(MessageFrame frame, IMEI imei) {
        // SK05 devices typically have standard login frames
        
        int frameLength = frame.getContent().readableBytes();
        
        if (frameLength >= 13 && frameLength <= 16) {
            logger.debug("🔍 Possible SK05 device detected based on standard login frame: {}", frameLength);
            return true;
        }
        
        return false;
    }

    /**
     * Provide device-specific configuration advice
     */
    private void provideDeviceConfigurationAdvice(String variant, String imei) {
        switch (variant.toUpperCase()) {
            case "V5" -> {
                logger.info("⚙️ V5 Device Configuration - IMEI: {}", imei);
                logger.info("    - V5 devices normally send status packets after login");
                logger.info("    - For location tracking: Move device or SMS 'tracker#123456#'");
                logger.info("    - Status packets indicate device is working properly");
            }
            case "SK05" -> {
                logger.info("⚙️ SK05 Device Configuration - IMEI: {}", imei);
                logger.info("    - Should send location packets immediately after login");
                logger.info("    - If no location: SMS 'upload_time#123456#30#'");
                logger.info("    - Check GPS antenna and signal strength");
            }
            default -> {
                logger.info("⚙️ GT06 Device Configuration - IMEI: {}", imei);
                logger.info("    - SMS: 'upload_time#123456#30#' (30-second intervals)");
                logger.info("    - SMS: 'tracker#123456#' (enable tracking)");
                logger.info("    - Move device to trigger GPS location");
            }
        }
    }

    /**
     * Handle unknown packets with analysis
     */
    private void handleUnknownPacket(ChannelHandlerContext ctx, MessageFrame frame) {
        String remoteAddress = ctx.channel().remoteAddress().toString();
        int protocolNumber = frame.getProtocolNumber();
        
        logger.warn("❓ Unknown packet analysis - Protocol: 0x{:02X}, Length: {}, From: {}", 
            protocolNumber, frame.getContent().readableBytes(), remoteAddress);
        
        // Always send ACK for unknown messages to keep device happy
        sendGenericAck(ctx, frame);
    }

    /**
     * Enhanced heartbeat handling
     */
    private void handleHeartbeat(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = sessionService.getSession(ctx.channel());
        
        if (sessionOpt.isPresent()) {
            DeviceSession session = sessionOpt.get();
            session.updateActivity();
            sessionService.saveSession(session);
            
            String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
            logger.info("💓 Heartbeat from IMEI: {} (Variant: {})", imei, session.getDeviceVariant());
        } else {
            logger.info("💓 Heartbeat from unknown session: {}", ctx.channel().remoteAddress());
        }
        
        sendGenericAck(ctx, frame);
    }

    /**
     * Handle LBS packets
     */
    private void handleLBSPacket(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = getAuthenticatedSession(ctx);
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No authenticated session for LBS from {}", ctx.channel().remoteAddress());
            return;
        }

        try {
            DeviceSession session = sessionOpt.get();
            String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
            
            logger.info("📶 Processing LBS packet for IMEI: {}", imei);
            
            telemetryService.processLBSMessage(session, frame);
            session.updateActivity();
            sessionService.saveSession(session);
            sendGenericAck(ctx, frame);
            
        } catch (Exception e) {
            logger.error("💥 Error handling LBS packet: {}", e.getMessage(), e);
            sendGenericAck(ctx, frame);
        }
    }

    /**
     * Handle command responses
     */
    private void handleCommandResponse(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = getAuthenticatedSession(ctx);
        if (sessionOpt.isPresent()) {
            DeviceSession session = sessionOpt.get();
            String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
            
            logger.info("📤 Command response from IMEI: {} (Serial: {})", imei, frame.getSerialNumber());
        }
        
        sendGenericAck(ctx, frame);
    }

    /**
     * Get authenticated session with enhanced validation - COMPATIBLE with your DeviceSessionService
     */
    private Optional<DeviceSession> getAuthenticatedSession(ChannelHandlerContext ctx) {
        try {
            Optional<DeviceSession> sessionOpt = sessionService.getSession(ctx.channel());
            
            if (sessionOpt.isEmpty()) {
                logger.debug("📭 No session found for channel from {}", ctx.channel().remoteAddress());
                return Optional.empty();
            }

            DeviceSession session = sessionOpt.get();
            String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
            
            if (!session.isAuthenticated()) {
                logger.warn("🔐 Session exists but NOT authenticated for IMEI: {} from {}", 
                    imei, ctx.channel().remoteAddress());
                return Optional.empty();
            }

            return sessionOpt;
            
        } catch (Exception e) {
            logger.error("💥 Error getting authenticated session: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Send acknowledgment with proper error handling
     */
    private void sendGenericAck(ChannelHandlerContext ctx, MessageFrame frame) {
        try {
            ByteBuf ack = protocolParser.buildGenericAck(frame.getProtocolNumber(), frame.getSerialNumber());
            
            logger.debug("📤 Sending ACK for protocol 0x{:02X}, serial {}", 
                frame.getProtocolNumber(), frame.getSerialNumber());
            
            ctx.writeAndFlush(ack).addListener(future -> {
                if (future.isSuccess()) {
                    logger.debug("✅ ACK sent successfully for protocol 0x{:02X}", frame.getProtocolNumber());
                } else {
                    logger.error("❌ Failed to send ACK for protocol 0x{:02X}: {}", 
                        frame.getProtocolNumber(), 
                        future.cause() != null ? future.cause().getMessage() : "Unknown error");
                }
            });
            
        } catch (Exception e) {
            logger.error("💥 Error building/sending ACK: {}", e.getMessage(), e);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent event) {
            if (event.state() == IdleState.ALL_IDLE) {
                logger.warn("⏱️ Connection idle timeout, closing: {}", ctx.channel().remoteAddress());
                ctx.close();
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        String remoteAddress = ctx.channel().remoteAddress().toString();
        String channelId = ctx.channel().id().asShortText();
        
        logger.info("🔌 Connection closed: {} (Channel ID: {})", remoteAddress, channelId);
        
        // Clean up resources
        channelRegistry.unregister(channelId);
        sessionService.removeSession(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("💥 Exception in GT06Handler from {}: {}", 
            ctx.channel().remoteAddress(), cause.getMessage(), cause);
        
        // Don't close connection for minor errors - GT06 devices are flaky
        if (cause instanceof java.io.IOException) {
            logger.warn("🔌 I/O exception, closing channel: {}", ctx.channel().remoteAddress());
            ctx.close();
        } else {
            logger.debug("🔄 Continuing after exception from: {}", ctx.channel().remoteAddress());
        }
    }
}
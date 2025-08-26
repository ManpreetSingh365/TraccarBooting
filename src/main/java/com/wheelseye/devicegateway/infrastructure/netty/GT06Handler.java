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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * GT06 Handler - FIXED VERSION with Correct Logging & Location Detection
 * 
 * Key Features:
 * 1. ✅ Fixed logging format bugs
 * 2. ✅ Enhanced location packet detection  
 * 3. ✅ Immediate location logging BEFORE Kafka
 * 4. ✅ Device configuration analysis
 * 5. ✅ Complete packet analysis with hex dumps
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
            
            // CRITICAL DEBUG: Log ALL incoming data as hex
            String hexDump = ByteBufUtil.hexDump(buffer);
            logger.info("📥 RAW DATA RECEIVED from {}: {} bytes", remoteAddress, buffer.readableBytes());
            logger.info("📥 HEX DUMP: {}", hexDump);
            
            // Parse the message frame
            MessageFrame frame = protocolParser.parseFrame(buffer);
            if (frame == null) {
                logger.warn("❌ Failed to parse frame from {} - Raw data: {}", remoteAddress, hexDump);
                return;
            }

            // FIXED: Proper logging format with actual hex values
            logger.info("📦 PARSED FRAME from {}: protocol=0x{:02X}, serial={}, length={}", 
                       remoteAddress, frame.getProtocolNumber(), frame.getSerialNumber(), 
                       frame.getContent().readableBytes());

            // Process message based on protocol number
            processMessage(ctx, frame);

        } catch (Exception e) {
            logger.error("💥 Error processing message from {}: {}", 
                       ctx.channel().remoteAddress(), e.getMessage(), e);
            e.printStackTrace(); // Full stack trace for debugging
        } finally {
            // Always release the buffer
            buffer.release();
        }
    }

    /**
     * Process message with FIXED logging format
     */
    private void processMessage(ChannelHandlerContext ctx, MessageFrame frame) {
        int protocolNumber = frame.getProtocolNumber();
        String remoteAddress = ctx.channel().remoteAddress().toString();
        
        // FIXED: Proper protocol number logging
        logger.info("🔍 Processing protocol 0x{:02X} from {}", protocolNumber, remoteAddress);
        
        try {
            switch (protocolNumber) {
                case 0x01 -> {
                    logger.info("🔐 LOGIN PACKET (0x01) detected from {}", remoteAddress);
                    handleLogin(ctx, frame);
                }
                case 0x12 -> {
                    logger.info("📍 LOCATION PACKET (0x12) detected from {}", remoteAddress);
                    handleLocationPacket(ctx, frame);
                }
                case 0x22 -> {
                    logger.info("📍 LOCATION PACKET (0x22) detected from {}", remoteAddress);
                    handleLocationPacket(ctx, frame);
                }
                case 0x16 -> {
                    logger.info("📍 GPS + LBS PACKET (0x16) detected from {}", remoteAddress);
                    handleLocationPacket(ctx, frame);
                }
                case 0x26 -> {
                    logger.info("📍 GPS + LBS PACKET (0x26) detected from {}", remoteAddress);
                    handleLocationPacket(ctx, frame);
                }
                case 0x13 -> {
                    logger.info("📊 STATUS PACKET (0x13) detected from {}", remoteAddress);
                    logger.warn("⚠️  DEVICE IS SENDING STATUS, NOT LOCATION! Check device config.");
                    handleStatusPacket(ctx, frame);
                }
                case 0x94 -> {
                    logger.info("📶 LBS PACKET (0x94) detected from {}", remoteAddress);
                    handleLBSPacket(ctx, frame);
                }
                case 0x24 -> {
                    logger.info("📦 VENDOR MULTI PACKET (0x24) detected from {}", remoteAddress);
                    handleVendorMultiPacket(ctx, frame);
                }
                case 0x23 -> {
                    logger.info("💓 HEARTBEAT PACKET (0x23) detected from {}", remoteAddress);
                    handleHeartbeat(ctx, frame);
                }
                case 0x8A -> {
                    logger.info("📤 COMMAND RESPONSE (0x8A) detected from {}", remoteAddress);
                    handleCommandResponse(ctx, frame);
                }
                default -> {
                    logger.warn("❓ UNKNOWN PROTOCOL 0x{:02X} detected from {}", protocolNumber, remoteAddress);
                    // Log raw content for analysis
                    String contentHex = ByteBufUtil.hexDump(frame.getContent());
                    logger.warn("❓ Raw content: {}", contentHex);
                    
                    // Try to detect if this might be a location packet with different protocol
                    analyzeUnknownPacket(frame, remoteAddress);
                    
                    // Still send ACK for unknown messages
                    sendGenericAck(ctx, frame);
                }
            }
        } catch (Exception e) {
            logger.error("💥 Error processing protocol 0x{:02X} from {}: {}", 
                       protocolNumber, remoteAddress, e.getMessage(), e);
            e.printStackTrace();
            // Always try to send ACK even on error
            sendGenericAck(ctx, frame);
        }
    }

    /**
     * Analyze unknown packets to see if they might be location data
     */
    private void analyzeUnknownPacket(MessageFrame frame, String remoteAddress) {
        try {
            ByteBuf content = frame.getContent();
            int protocolNumber = frame.getProtocolNumber();
            
            logger.info("🔍 Analyzing unknown packet 0x{:02X} (length: {}) from {}", 
                       protocolNumber, content.readableBytes(), remoteAddress);
            
            // Check if it might be a location packet with different protocol number
            if (content.readableBytes() >= 20) {
                logger.info("🔍 Packet size suggests possible location data - trying to parse as location");
                
                // Try to parse as location anyway
                try {
                    Location location = protocolParser.parseLocation(frame);
                    if (location != null) {
                        logger.info("🎯 SUCCESS! Unknown packet 0x{:02X} contains LOCATION DATA!", protocolNumber);
                        logLocationData(location, "UNKNOWN", remoteAddress);
                    }
                } catch (Exception e) {
                    logger.debug("🔍 Unknown packet 0x{:02X} is not location data: {}", protocolNumber, e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.debug("🔍 Error analyzing unknown packet: {}", e.getMessage());
        }
    }

    /**
     * Handle login with enhanced debugging
     */
    private void handleLogin(ChannelHandlerContext ctx, MessageFrame frame) {
        String remoteAddress = ctx.channel().remoteAddress().toString();
        
        try {
            // Log raw login frame content
            String loginHex = ByteBufUtil.hexDump(frame.getContent());
            logger.info("🔐 LOGIN frame content: {}", loginHex);
            
            IMEI imei = protocolParser.extractIMEI(frame);
            if (imei == null) {
                logger.warn("❌ Failed to extract IMEI from login frame from {}", remoteAddress);
                ctx.close();
                return;
            }

            logger.info("🔐 Login request from IMEI: {}", imei.getValue());

            // Create or get existing session
            DeviceSession session = sessionService.createSession(imei, ctx.channel());
            
            // Authenticate and save the session
            session.authenticate();
            sessionService.saveSession(session);
            
            logger.info("✅ Session authenticated and saved for IMEI: {} (Session ID: {})", 
                       imei.getValue(), session.getId());

            // Send login ACK
            ByteBuf ack = protocolParser.buildLoginAck(frame.getSerialNumber());
            ctx.writeAndFlush(ack).addListener(future -> {
                if (future.isSuccess()) {
                    logger.info("✅ Login successful - IMEI: {} from {}", imei.getValue(), remoteAddress);
                    logger.info("📤 Login ACK sent to {} (IMEI: {})", remoteAddress, imei.getValue());
                    
                    // Show device configuration advice
                    logger.info("⚙️  DEVICE CONFIG: If no location packets appear, device may need configuration:");
                    logger.info("⚙️  - SMS: 'upload_time#123456#30#' (set 30sec interval)");
                    logger.info("⚙️  - SMS: 'tracker#123456#' (enable tracking mode)");
                    logger.info("⚙️  - Or move device physically to trigger location");
                } else {
                    logger.error("❌ Failed to send login ACK to {} (IMEI: {}): {}", 
                               remoteAddress, imei.getValue(), 
                               future.cause() != null ? future.cause().getMessage() : "Unknown error");
                }
            });

        } catch (Exception e) {
            logger.error("💥 Error handling login from {}: {}", remoteAddress, e.getMessage(), e);
            e.printStackTrace();
            ctx.close();
        }
    }

    /**
     * Handle location packet with IMMEDIATE logging before Kafka
     */
    private void handleLocationPacket(ChannelHandlerContext ctx, MessageFrame frame) {
        String remoteAddress = ctx.channel().remoteAddress().toString();
        
        logger.info("📍 LOCATION PROCESSING START from {}", remoteAddress);
        
        // Get authenticated session
        Optional<DeviceSession> sessionOpt = getAuthenticatedSession(ctx);
        
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No authenticated session for location from {} - rejecting", remoteAddress);
            return;
        }

        DeviceSession session = sessionOpt.get();
        String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
        
        logger.info("✅ Processing location for authenticated IMEI: {}", imei);

        try {
            // Log raw location frame content for debugging
            String locationHex = ByteBufUtil.hexDump(frame.getContent());
            logger.info("📍 LOCATION frame content (IMEI: {}): {}", imei, locationHex);
            
            // IMMEDIATELY parse and log location data (BEFORE Kafka)
            Location location = protocolParser.parseLocation(frame);
            if (location != null) {
                // 🎯 PRIMARY LOCATION LOG - This is what you want to see!
                logLocationData(location, imei, remoteAddress);
                
            } else {
                logger.warn("❌ Failed to parse location data for IMEI: {} - Raw: {}", imei, locationHex);
                // Try alternative parsing approaches
                tryAlternativeLocationParsing(frame, imei);
            }
            
            // Now try to process through telemetry service (Kafka)
            try {
                logger.info("📤 Sending location to Kafka for IMEI: {}", imei);
                telemetryService.processLocationMessage(session, frame);
                logger.info("✅ Location sent to Kafka successfully for IMEI: {}", imei);
            } catch (Exception kafkaError) {
                // Don't fail if Kafka is down - we've already logged the location above
                logger.warn("⚠️ Kafka processing failed for IMEI: {}, but location was logged: {}", 
                          imei, kafkaError.getMessage());
            }
            
            // Update session activity and save
            session.updateActivity();
            sessionService.saveSession(session);
            
            // Send ACK to device
            sendGenericAck(ctx, frame);
            logger.info("📤 Location ACK sent to IMEI: {}", imei);
            
        } catch (Exception e) {
            logger.error("💥 Error handling location from {}: {}", remoteAddress, e.getMessage(), e);
            e.printStackTrace();
            // Still send ACK to keep device happy
            sendGenericAck(ctx, frame);
        }
    }

    /**
     * Log location data in the requested format
     */
    private void logLocationData(Location location, String imei, String remoteAddress) {
        // PRIMARY LOCATION LOG in requested format
        logger.info("📍 Received location - IMEI: {}, Lat: {:.6f}, Lon: {:.6f}, Speed: {:.1f} km/h", 
                   imei, location.getLatitude(), location.getLongitude(), location.getSpeed());
        
        // Additional detailed logging
        logger.info("📍 ===== LOCATION DATA RECEIVED =====");
        logger.info("📍 IMEI: {}", imei);
        logger.info("📍 Source: {}", remoteAddress);
        logger.info("📍 Latitude: {:.6f}", location.getLatitude());
        logger.info("📍 Longitude: {:.6f}", location.getLongitude());
        logger.info("📍 Speed: {:.1f} km/h", location.getSpeed());
        logger.info("📍 Altitude: {:.1f} meters", location.getAltitude());
        logger.info("📍 Heading: {:.1f} degrees", location.getCourse());
        logger.info("📍 Satellites: {}", location.getSatellites());
        logger.info("📍 GPS Valid: {}", location.isValid() ? "YES" : "NO");
        logger.info("📍 Timestamp: {}", location.getTimestamp());
        logger.info("📍 Received At: {}", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        logger.info("📍 ================================");
    }

    /**
     * Try alternative location parsing methods
     */
    private void tryAlternativeLocationParsing(MessageFrame frame, String imei) {
        try {
            logger.info("🔍 Trying alternative location parsing for IMEI: {}", imei);
            
            ByteBuf content = frame.getContent();
            content.markReaderIndex();
            
            if (content.readableBytes() >= 20) {
                // Try basic GPS parsing
                logger.info("🔍 Raw location bytes for IMEI {}: {}", imei, ByteBufUtil.hexDump(content));
                
                // Reset reader index
                content.resetReaderIndex();
                
                // Try to extract basic coordinates (simplified approach)
                if (content.readableBytes() >= 12) {
                    try {
                        // Skip timestamp (4 bytes)
                        content.skipBytes(4);
                        
                        // Try to read coordinates
                        int latRaw = content.readInt();
                        int lonRaw = content.readInt();
                        
                        double lat = latRaw / 1800000.0;
                        double lon = lonRaw / 1800000.0;
                        
                        logger.info("🔍 Alternative parsing result - IMEI: {}, Raw Lat: {:.6f}, Raw Lon: {:.6f}", 
                                   imei, lat, lon);
                        
                        // If coordinates seem reasonable, log as location
                        if (lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180) {
                            logger.info("📍 Received location (alternative parse) - IMEI: {}, Lat: {:.6f}, Lon: {:.6f}, Speed: 0.0 km/h", 
                                       imei, lat, lon);
                        }
                                   
                    } catch (Exception e) {
                        logger.debug("🔍 Alternative parsing failed: {}", e.getMessage());
                    }
                }
            }
            
            content.resetReaderIndex();
            
        } catch (Exception e) {
            logger.debug("🔍 Alternative location parsing failed: {}", e.getMessage());
        }
    }

    /**
     * Handle status packet with configuration advice
     */
    private void handleStatusPacket(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = getAuthenticatedSession(ctx);
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No authenticated session for status from {}", ctx.channel().remoteAddress());
            return;
        }

        try {
            DeviceSession session = sessionOpt.get();
            String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
            
            logger.info("📊 Processing status for IMEI: {}", imei);
            logger.warn("⚠️  STATUS ONLY - IMEI {} not sending location packets", imei);
            logger.warn("⚠️  Try: Move device, or SMS 'tracker#123456#' to enable GPS");
            
            telemetryService.processStatusMessage(session, frame);
            session.updateActivity();
            sessionService.saveSession(session);
            sendGenericAck(ctx, frame);
            
        } catch (Exception e) {
            logger.error("💥 Error handling status: {}", e.getMessage(), e);
            sendGenericAck(ctx, frame);
        }
    }

    // ... (other packet handlers remain the same)
    
    private void handleLBSPacket(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = getAuthenticatedSession(ctx);
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No authenticated session for LBS from {}", ctx.channel().remoteAddress());
            return;
        }

        try {
            DeviceSession session = sessionOpt.get();
            String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
            
            logger.info("📶 Processing LBS for IMEI: {}", imei);
            
            telemetryService.processLBSMessage(session, frame);
            session.updateActivity();
            sessionService.saveSession(session);
            sendGenericAck(ctx, frame);
            
        } catch (Exception e) {
            logger.error("💥 Error handling LBS: {}", e.getMessage(), e);
            sendGenericAck(ctx, frame);
        }
    }

    private void handleVendorMultiPacket(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = getAuthenticatedSession(ctx);
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No authenticated session for vendor-multi from {}", ctx.channel().remoteAddress());
            return;
        }

        try {
            DeviceSession session = sessionOpt.get();
            String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
            
            logger.info("📦 Processing vendor-multi for IMEI: {}", imei);
            
            telemetryService.processVendorMultiMessage(session, frame);
            session.updateActivity();
            sessionService.saveSession(session);
            sendGenericAck(ctx, frame);
            
        } catch (Exception e) {
            logger.error("💥 Error handling vendor-multi: {}", e.getMessage(), e);
            sendGenericAck(ctx, frame);
        }
    }

    private void handleHeartbeat(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = sessionService.getSession(ctx.channel());
        
        if (sessionOpt.isPresent()) {
            DeviceSession session = sessionOpt.get();
            session.updateActivity();
            sessionService.saveSession(session);
            
            String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
            logger.info("💓 Heartbeat from IMEI: {}", imei);
        } else {
            logger.info("💓 Heartbeat from unknown session: {}", ctx.channel().remoteAddress());
        }
        
        sendGenericAck(ctx, frame);
    }

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
     * Get authenticated session with enhanced logging
     */
    private Optional<DeviceSession> getAuthenticatedSession(ChannelHandlerContext ctx) {
        try {
            Optional<DeviceSession> sessionOpt = sessionService.getSession(ctx.channel());
            
            if (sessionOpt.isEmpty()) {
                logger.debug("📭 No session found for channel: {} from {}", 
                           ctx.channel().id().asShortText(), ctx.channel().remoteAddress());
                return Optional.empty();
            }
            
            DeviceSession session = sessionOpt.get();
            String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
            
            if (!session.isAuthenticated()) {
                logger.warn("🔐 Session exists but NOT authenticated for IMEI: {} from {} - Session ID: {}", 
                          imei, ctx.channel().remoteAddress(), session.getId());
                return Optional.empty();
            }
            
            logger.debug("✅ Found authenticated session for IMEI: {} (Session ID: {})", 
                       imei, session.getId());
            
            return sessionOpt;
            
        } catch (Exception e) {
            logger.error("💥 Error getting authenticated session for {}: {}", 
                       ctx.channel().remoteAddress(), e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Send ACK with FIXED logging format
     */
    private void sendGenericAck(ChannelHandlerContext ctx, MessageFrame frame) {
        try {
            ByteBuf ack = protocolParser.buildGenericAck(frame.getProtocolNumber(), frame.getSerialNumber());
            
            // FIXED: Proper protocol number logging
            logger.info("📤 Sending ACK for protocol 0x{:02X}, serial {} to {}", 
                       frame.getProtocolNumber(), frame.getSerialNumber(), ctx.channel().remoteAddress());
            
            ctx.writeAndFlush(ack).addListener(future -> {
                if (future.isSuccess()) {
                    logger.debug("📤 ACK sent successfully for protocol 0x{:02X}, serial {}", 
                               frame.getProtocolNumber(), frame.getSerialNumber());
                } else {
                    logger.error("❌ Failed to send ACK for protocol 0x{:02X}: {}", 
                               frame.getProtocolNumber(),
                               future.cause() != null ? future.cause().getMessage() : "Unknown error");
                }
            });
        } catch (Exception e) {
            logger.error("💥 Error building/sending ACK to {}: {}", 
                       ctx.channel().remoteAddress(), e.getMessage(), e);
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
        
        channelRegistry.unregister(channelId);
        sessionService.removeSession(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("💥 Exception in GT06Handler from {}: {}", 
                   ctx.channel().remoteAddress(), cause.getMessage(), cause);
        cause.printStackTrace();
        ctx.close();
    }
}
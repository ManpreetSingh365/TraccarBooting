package com.wheelseye.devicegateway.infrastructure.netty;

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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * GT06 Handler - COMPLETE FIX for Authentication and Location Processing
 * 
 * Key Fixes:
 * 1. ✅ Proper session authentication persistence
 * 2. ✅ Enhanced location packet processing with detailed logging
 * 3. ✅ Kafka-independent logging for debugging
 * 4. ✅ Better error handling and validation
 * 5. ✅ Channel registry integration
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
            // Parse the message frame first
            MessageFrame frame = protocolParser.parseFrame(buffer);
            if (frame == null) {
                logger.warn("❌ Failed to parse frame from {}", ctx.channel().remoteAddress());
                return;
            }

            logger.debug("📦 Received frame: protocol=0x{:02X}, serial={}, length={} from {}", 
                       frame.getProtocolNumber(), frame.getSerialNumber(), 
                       frame.getContent().readableBytes(), ctx.channel().remoteAddress());

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
     * Process message based on protocol number - ENHANCED with better logging
     */
    private void processMessage(ChannelHandlerContext ctx, MessageFrame frame) {
        int protocolNumber = frame.getProtocolNumber();
        
        try {
            switch (protocolNumber) {
                case 0x01 -> {
                    // Login - handle directly (creates session)
                    logger.info("🔐 Processing login from {}", ctx.channel().remoteAddress());
                    handleLogin(ctx, frame);
                }
                case 0x12, 0x22 -> {
                    // Location (GPS) - ENHANCED processing with detailed logging
                    logger.info("📍 Processing location packet (0x{:02X}) from {}", 
                              protocolNumber, ctx.channel().remoteAddress());
                    handleLocationPacket(ctx, frame);
                }
                case 0x13 -> {
                    // Status
                    logger.debug("📊 Processing status from {}", ctx.channel().remoteAddress());
                    handleStatusPacket(ctx, frame);
                }
                case 0x94 -> {
                    // LBS Network
                    logger.debug("📶 Processing LBS from {}", ctx.channel().remoteAddress());
                    handleLBSPacket(ctx, frame);
                }
                case 0x24 -> {
                    // Vendor Multi
                    logger.debug("📦 Processing vendor-multi from {}", ctx.channel().remoteAddress());
                    handleVendorMultiPacket(ctx, frame);
                }
                case 0x23 -> {
                    // Heartbeat
                    logger.debug("💓 Processing heartbeat from {}", ctx.channel().remoteAddress());
                    handleHeartbeat(ctx, frame);
                }
                default -> {
                    logger.warn("❓ Unknown protocol number: 0x{:02X} from {}", 
                              protocolNumber, ctx.channel().remoteAddress());
                    // Still send ACK for unknown messages
                    sendGenericAck(ctx, frame);
                }
            }
        } catch (Exception e) {
            logger.error("💥 Error processing protocol 0x{:02X} from {}: {}", 
                       protocolNumber, ctx.channel().remoteAddress(), e.getMessage(), e);
            // Always try to send ACK even on error
            sendGenericAck(ctx, frame);
        }
    }

    /**
     * Handle login message - FIXED authentication persistence
     */
    private void handleLogin(ChannelHandlerContext ctx, MessageFrame frame) {
        try {
            IMEI imei = protocolParser.extractIMEI(frame);
            if (imei == null) {
                logger.warn("❌ Failed to extract IMEI from login frame from {}", 
                          ctx.channel().remoteAddress());
                ctx.close();
                return;
            }

            logger.info("🔐 Login request from IMEI: {}", imei.getValue());

            // Create or get existing session
            DeviceSession session = sessionService.createSession(imei, ctx.channel());
            
            // CRITICAL FIX: Authenticate and save the session properly
            session.authenticate();
            sessionService.saveSession(session);  // This was missing!
            
            logger.info("✅ Session authenticated and saved for IMEI: {} (Session ID: {})", 
                       imei.getValue(), session.getId());

            // Send login ACK
            ByteBuf ack = protocolParser.buildLoginAck(frame.getSerialNumber());
            ctx.writeAndFlush(ack).addListener(future -> {
                if (future.isSuccess()) {
                    logger.info("✅ Login successful - IMEI: {} from {}", 
                              imei.getValue(), ctx.channel().remoteAddress());
                    logger.info("📤 Login ACK sent to {} (IMEI: {})", 
                              ctx.channel().remoteAddress(), imei.getValue());
                } else {
                    logger.error("❌ Failed to send login ACK to {} (IMEI: {}): {}", 
                               ctx.channel().remoteAddress(), imei.getValue(), 
                               future.cause() != null ? future.cause().getMessage() : "Unknown error");
                }
            });

        } catch (Exception e) {
            logger.error("💥 Error handling login from {}: {}", 
                       ctx.channel().remoteAddress(), e.getMessage(), e);
            ctx.close();
        }
    }

    /**
     * Handle location packet - ENHANCED with detailed logging and Kafka-independent processing
     */
    private void handleLocationPacket(ChannelHandlerContext ctx, MessageFrame frame) {
        // Get authenticated session
        Optional<DeviceSession> sessionOpt = getAuthenticatedSession(ctx);
        
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No authenticated session found for location from {} - rejecting packet", 
                      ctx.channel().remoteAddress());
            return;
        }

        try {
            DeviceSession session = sessionOpt.get();
            String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
            
            logger.info("✅ Processing location for authenticated IMEI: {}", imei);
            
            // Parse location data directly for logging (even if Kafka fails)
            Location location = protocolParser.parseLocation(frame);
            if (location != null) {
                // LOG LOCATION DATA IMMEDIATELY (Kafka-independent)
                logger.info("📍 Location received - IMEI: {}, Lat: {:.6f}, Lon: {:.6f}, Speed: {:.1f} km/h, Valid: {}, Time: {}", 
                          imei, 
                          location.getLatitude(), 
                          location.getLongitude(), 
                          location.getSpeed(),
                          location.isValid() ? "GPS" : "Invalid",
                          location.getTimestamp());
                          
                // Additional detailed logging
                logger.debug("📍 Location details - IMEI: {}, Altitude: {:.1f}m, Heading: {:.1f}°, Satellites: {}", 
                           imei,
                           location.getAltitude(),
                           location.getCourse(),
                           location.getSatellites());
            } else {
                logger.warn("❌ Failed to parse location data for IMEI: {}", imei);
            }
            
            // Try to process through telemetry service (may use Kafka)
            try {
                telemetryService.processLocationMessage(session, frame);
                logger.debug("✅ Location sent to telemetry service for IMEI: {}", imei);
            } catch (Exception kafkaError) {
                // Don't fail if Kafka is down - we've already logged the location
                logger.warn("⚠️ Telemetry service failed for IMEI: {} (Kafka issue?): {}", 
                          imei, kafkaError.getMessage());
            }
            
            // Update session activity and save
            session.updateActivity();
            sessionService.saveSession(session);
            
            // Send ACK to device
            sendGenericAck(ctx, frame);
            logger.debug("📤 Location ACK sent to IMEI: {}", imei);
            
        } catch (Exception e) {
            logger.error("💥 Error handling location from {}: {}", 
                       ctx.channel().remoteAddress(), e.getMessage(), e);
            // Still send ACK to keep device happy
            sendGenericAck(ctx, frame);
        }
    }

    /**
     * Handle status packet - ENHANCED
     */
    private void handleStatusPacket(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = getAuthenticatedSession(ctx);
        
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No authenticated session found for status from {}", 
                      ctx.channel().remoteAddress());
            return;
        }

        try {
            DeviceSession session = sessionOpt.get();
            String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
            
            logger.debug("📊 Processing status for IMEI: {}", imei);
            
            // Process status data
            try {
                telemetryService.processStatusMessage(session, frame);
                logger.debug("✅ Status processed for IMEI: {}", imei);
            } catch (Exception e) {
                logger.warn("⚠️ Status processing failed for IMEI: {}: {}", imei, e.getMessage());
            }
            
            // Update session activity and save
            session.updateActivity();
            sessionService.saveSession(session);
            
            // Send ACK
            sendGenericAck(ctx, frame);
            
        } catch (Exception e) {
            logger.error("💥 Error handling status from {}: {}", 
                       ctx.channel().remoteAddress(), e.getMessage(), e);
            sendGenericAck(ctx, frame);
        }
    }

    /**
     * Handle LBS packet - ENHANCED
     */
    private void handleLBSPacket(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = getAuthenticatedSession(ctx);
        
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No authenticated session found for LBS from {}", 
                      ctx.channel().remoteAddress());
            return;
        }

        try {
            DeviceSession session = sessionOpt.get();
            String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
            
            logger.debug("📶 Processing LBS for IMEI: {}", imei);
            
            // Process LBS data  
            try {
                telemetryService.processLBSMessage(session, frame);
                logger.debug("✅ LBS processed for IMEI: {}", imei);
            } catch (Exception e) {
                logger.warn("⚠️ LBS processing failed for IMEI: {}: {}", imei, e.getMessage());
            }
            
            // Update session activity and save
            session.updateActivity();
            sessionService.saveSession(session);
            
            // Send ACK
            sendGenericAck(ctx, frame);
            
        } catch (Exception e) {
            logger.error("💥 Error handling LBS from {}: {}", 
                       ctx.channel().remoteAddress(), e.getMessage(), e);
            sendGenericAck(ctx, frame);
        }
    }

    /**
     * Handle vendor multi packet - ENHANCED
     */
    private void handleVendorMultiPacket(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = getAuthenticatedSession(ctx);
        
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No authenticated session found for vendor-multi from {}", 
                      ctx.channel().remoteAddress());
            return;
        }

        try {
            DeviceSession session = sessionOpt.get();
            String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
            
            logger.debug("📦 Processing vendor-multi for IMEI: {}", imei);
            
            // Process vendor-specific multi-record data
            try {
                telemetryService.processVendorMultiMessage(session, frame);
                logger.debug("✅ Vendor-multi processed for IMEI: {}", imei);
            } catch (Exception e) {
                logger.warn("⚠️ Vendor-multi processing failed for IMEI: {}: {}", imei, e.getMessage());
            }
            
            // Update session activity and save
            session.updateActivity();
            sessionService.saveSession(session);
            
            // Send ACK
            sendGenericAck(ctx, frame);
            
        } catch (Exception e) {
            logger.error("💥 Error handling vendor-multi from {}: {}", 
                       ctx.channel().remoteAddress(), e.getMessage(), e);
            sendGenericAck(ctx, frame);
        }
    }

    /**
     * Handle heartbeat
     */
    private void handleHeartbeat(ChannelHandlerContext ctx, MessageFrame frame) {
        // Heartbeat doesn't require authentication, just session existence
        Optional<DeviceSession> sessionOpt = sessionService.getSession(ctx.channel());
        
        if (sessionOpt.isPresent()) {
            DeviceSession session = sessionOpt.get();
            session.updateActivity();
            sessionService.saveSession(session);
            
            String imei = session.getImei() != null ? session.getImei().getValue() : "unknown";
            logger.debug("💓 Heartbeat from IMEI: {}", imei);
        } else {
            logger.debug("💓 Heartbeat from unknown session: {}", ctx.channel().remoteAddress());
        }
        
        // Always send heartbeat ACK
        sendGenericAck(ctx, frame);
    }

    /**
     * Get authenticated session with enhanced validation and logging
     */
    private Optional<DeviceSession> getAuthenticatedSession(ChannelHandlerContext ctx) {
        try {
            // Get session for this channel
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
     * Send generic ACK response with enhanced logging
     */
    private void sendGenericAck(ChannelHandlerContext ctx, MessageFrame frame) {
        try {
            ByteBuf ack = protocolParser.buildGenericAck(frame.getProtocolNumber(), frame.getSerialNumber());
            ctx.writeAndFlush(ack).addListener(future -> {
                if (future.isSuccess()) {
                    logger.debug("📤 ACK sent for protocol 0x{:02X}, serial {}", 
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
        
        // Unregister channel from registry
        channelRegistry.unregister(channelId);
        logger.debug("🗑️ Channel unregistered from registry: {}", channelId);
        
        // Remove session
        sessionService.removeSession(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("💥 Exception in GT06Handler from {}: {}", 
                   ctx.channel().remoteAddress(), cause.getMessage(), cause);
        
        // Close channel for serious errors  
        ctx.close();
    }
}
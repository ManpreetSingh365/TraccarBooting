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
import com.wheelseye.devicegateway.domain.valueobjects.MessageFrame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * GT06 Handler - FIXED Authentication Issue
 * 
 * Key Fixes:
 * 1. ✅ Proper session authentication and persistence
 * 2. ✅ Session state properly saved after authentication
 * 3. ✅ Channel registration with ChannelRegistry  
 * 4. ✅ Enhanced session validation for location packets
 * 5. ✅ Better error handling and logging
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

            logger.debug("📦 Received frame: protocol=0x{:02X}, serial={} from {}", 
                       frame.getProtocolNumber(), frame.getSerialNumber(), ctx.channel().remoteAddress());

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
     * Process message based on protocol number - FIXED session handling
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
                    // Location (GPS) - requires authenticated session
                    logger.debug("📍 Processing location from {}", ctx.channel().remoteAddress());
                    handleLocationWithValidation(ctx, frame);
                }
                case 0x13 -> {
                    // Status - requires session
                    logger.debug("📊 Processing status from {}", ctx.channel().remoteAddress());
                    handleStatusWithValidation(ctx, frame);
                }
                case 0x94 -> {
                    // LBS Network - requires session
                    logger.debug("📶 Processing LBS from {}", ctx.channel().remoteAddress());
                    handleLBSWithValidation(ctx, frame);
                }
                case 0x24 -> {
                    // Vendor Multi - requires session
                    logger.debug("📦 Processing vendor-multi from {}", ctx.channel().remoteAddress());
                    handleVendorMultiWithValidation(ctx, frame);
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
     * Handle login message - FIXED to properly persist authentication
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
            
            // CRITICAL FIX: Authenticate and save the session
            session.authenticate();
            sessionService.saveSession(session); // This was missing!
            
            logger.info("✅ Session authenticated and saved for IMEI: {}", imei.getValue());

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
     * Handle location with proper session validation - FIXED
     */
    private void handleLocationWithValidation(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = getAuthenticatedSession(ctx);
        
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No authenticated session found for location from {}", 
                      ctx.channel().remoteAddress());
            return;
        }

        try {
            DeviceSession session = sessionOpt.get();
            logger.debug("✅ Processing location for authenticated IMEI: {}", 
                       session.getImei().getValue());
            
            // Process location data
            telemetryService.processLocationMessage(session, frame);
            
            // Update session activity and save
            session.updateActivity();
            sessionService.saveSession(session);
            
            // Send ACK
            sendGenericAck(ctx, frame);
            
        } catch (Exception e) {
            logger.error("💥 Error handling location from {}: {}", 
                       ctx.channel().remoteAddress(), e.getMessage(), e);
            sendGenericAck(ctx, frame);
        }
    }

    /**
     * Handle status with validation - FIXED
     */
    private void handleStatusWithValidation(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = getAuthenticatedSession(ctx);
        
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No authenticated session found for status from {}", 
                      ctx.channel().remoteAddress());
            return;
        }

        try {
            DeviceSession session = sessionOpt.get();
            
            // Process status data
            telemetryService.processStatusMessage(session, frame);
            
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
     * Handle LBS with validation - FIXED
     */
    private void handleLBSWithValidation(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = getAuthenticatedSession(ctx);
        
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No authenticated session found for LBS from {}", 
                      ctx.channel().remoteAddress());
            return;
        }

        try {
            DeviceSession session = sessionOpt.get();
            
            // Process LBS data  
            telemetryService.processLBSMessage(session, frame);
            
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
     * Handle vendor multi with validation - FIXED
     */
    private void handleVendorMultiWithValidation(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = getAuthenticatedSession(ctx);
        
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No authenticated session found for vendor-multi from {}", 
                      ctx.channel().remoteAddress());
            return;
        }

        try {
            DeviceSession session = sessionOpt.get();
            
            // Process vendor-specific multi-record data
            telemetryService.processVendorMultiMessage(session, frame);
            
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
            
            logger.debug("💓 Heartbeat from IMEI: {}", 
                       session.getImei() != null ? session.getImei().getValue() : "unknown");
        }
        
        // Always send heartbeat ACK
        sendGenericAck(ctx, frame);
    }

    /**
     * Get authenticated session with enhanced validation - NEW METHOD
     */
    private Optional<DeviceSession> getAuthenticatedSession(ChannelHandlerContext ctx) {
        try {
            // Get session for this channel
            Optional<DeviceSession> sessionOpt = sessionService.getSession(ctx.channel());
            
            if (sessionOpt.isEmpty()) {
                logger.debug("📭 No session found for channel: {}", ctx.channel().id().asShortText());
                return Optional.empty();
            }
            
            DeviceSession session = sessionOpt.get();
            
            if (!session.isAuthenticated()) {
                logger.warn("🔐 Session exists but not authenticated for IMEI: {} from {}", 
                          session.getImei() != null ? session.getImei().getValue() : "unknown",
                          ctx.channel().remoteAddress());
                return Optional.empty();
            }
            
            logger.debug("✅ Found authenticated session for IMEI: {}", 
                       session.getImei() != null ? session.getImei().getValue() : "unknown");
            
            return sessionOpt;
            
        } catch (Exception e) {
            logger.error("💥 Error getting authenticated session for {}: {}", 
                       ctx.channel().remoteAddress(), e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Send generic ACK response
     */
    private void sendGenericAck(ChannelHandlerContext ctx, MessageFrame frame) {
        try {
            ByteBuf ack = protocolParser.buildGenericAck(frame.getProtocolNumber(), frame.getSerialNumber());
            ctx.writeAndFlush(ack).addListener(future -> {
                if (future.isSuccess()) {
                    logger.debug("📤 ACK sent for protocol 0x{:02X}", frame.getProtocolNumber());
                } else {
                    logger.error("❌ Failed to send ACK: {}", 
                               future.cause() != null ? future.cause().getMessage() : "Unknown error");
                }
            });
        } catch (Exception e) {
            logger.error("💥 Error sending ACK to {}: {}", 
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
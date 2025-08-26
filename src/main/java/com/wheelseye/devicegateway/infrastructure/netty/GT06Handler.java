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
 * GT06 Handler - Enhanced with ChannelRegistry integration
 * 
 * Key Features:
 * 1. ✅ Registers channels with ChannelRegistry for command delivery
 * 2. ✅ Uses DeviceSessionService for session management  
 * 3. ✅ Enhanced logging with emojis
 * 4. ✅ Proper channel lifecycle management
 * 5. ✅ Modern Java 21 switch expressions
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
            // Get session for this channel
            Optional<DeviceSession> sessionOpt = sessionService.getSession(ctx.channel());
            
            // Parse the message frame
            MessageFrame frame = protocolParser.parseFrame(buffer);
            if (frame == null) {
                logger.warn("❌ Failed to parse frame from {}", ctx.channel().remoteAddress());
                return;
            }

            // Update session activity if session exists
            sessionOpt.ifPresent(DeviceSession::updateActivity);

            // Process message based on protocol number
            processMessage(ctx, frame, sessionOpt);

        } catch (Exception e) {
            logger.error("💥 Error processing message from {}: {}", 
                       ctx.channel().remoteAddress(), e.getMessage(), e);
        } finally {
            // Always release the buffer
            buffer.release();
        }
    }

    /**
     * Process message based on protocol number using modern switch
     */
    private void processMessage(ChannelHandlerContext ctx, MessageFrame frame, Optional<DeviceSession> sessionOpt) {
        int protocolNumber = frame.getProtocolNumber();
        
        try {
            switch (protocolNumber) {
                case 0x01 -> {
                    // Login
                    logger.info("🔐 Processing login from {}", ctx.channel().remoteAddress());
                    handleLogin(ctx, frame);
                }
                case 0x12, 0x22 -> {
                    // Location (GPS)
                    logger.debug("📍 Processing location from {}", ctx.channel().remoteAddress());
                    handleLocation(ctx, frame, sessionOpt);
                }
                case 0x13 -> {
                    // Status
                    logger.debug("📊 Processing status from {}", ctx.channel().remoteAddress());
                    handleStatus(ctx, frame, sessionOpt);
                }
                case 0x94 -> {
                    // LBS Network
                    logger.debug("📶 Processing LBS from {}", ctx.channel().remoteAddress());
                    handleLBSNetwork(ctx, frame, sessionOpt);
                }
                case 0x24 -> {
                    // Vendor Multi
                    logger.debug("📦 Processing vendor-multi from {}", ctx.channel().remoteAddress());
                    handleVendorMulti(ctx, frame, sessionOpt);
                }
                case 0x8A -> {
                    // Command Response
                    logger.debug("📤 Processing command response from {}", ctx.channel().remoteAddress());
                    handleCommandResponse(ctx, frame, sessionOpt);
                }
                default -> {
                    logger.warn("❓ Unknown protocol number: 0x{:02X} from {}", 
                              protocolNumber, ctx.channel().remoteAddress());
                    // Still try to send ACK for unknown messages
                    sendGenericAck(ctx, frame);
                }
            }
        } catch (Exception e) {
            logger.error("💥 Error processing protocol 0x{:02X} from {}: {}", 
                       protocolNumber, ctx.channel().remoteAddress(), e.getMessage(), e);
        }
    }

    /**
     * Handle login message
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

            // Create or update session
            DeviceSession session = sessionService.createSession(imei, ctx.channel());
            session.authenticate();
            
            // Update session in repository
            // The sessionService.createSession already saves it, but we authenticate after
            sessionService.updateActivity(ctx.channel());

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
     * Handle location message
     */
    private void handleLocation(ChannelHandlerContext ctx, MessageFrame frame, Optional<DeviceSession> sessionOpt) {
        if (sessionOpt.isEmpty() || !sessionOpt.get().isAuthenticated()) {
            logger.warn("❌ Received location from unauthenticated session: {}", 
                      ctx.channel().remoteAddress());
            return;
        }

        try {
            // Process location data
            telemetryService.processLocationMessage(sessionOpt.get(), frame);
            
            // Send ACK
            sendGenericAck(ctx, frame);
            
        } catch (Exception e) {
            logger.error("💥 Error handling location from {}: {}", 
                       ctx.channel().remoteAddress(), e.getMessage(), e);
            // Still try to send ACK
            sendGenericAck(ctx, frame);
        }
    }

    /**
     * Handle status message
     */
    private void handleStatus(ChannelHandlerContext ctx, MessageFrame frame, Optional<DeviceSession> sessionOpt) {
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No session found for status message from {}", 
                      ctx.channel().remoteAddress());
            return;
        }

        try {
            // Process status data
            telemetryService.processStatusMessage(sessionOpt.get(), frame);
            
            // Send ACK
            sendGenericAck(ctx, frame);
            
        } catch (Exception e) {
            logger.error("💥 Error handling status from {}: {}", 
                       ctx.channel().remoteAddress(), e.getMessage(), e);
            // Still try to send ACK
            sendGenericAck(ctx, frame);
        }
    }

    /**
     * Handle LBS network message
     */
    private void handleLBSNetwork(ChannelHandlerContext ctx, MessageFrame frame, Optional<DeviceSession> sessionOpt) {
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No session found for LBS message from {}", 
                      ctx.channel().remoteAddress());
            return;
        }

        try {
            // Process LBS data
            telemetryService.processLBSMessage(sessionOpt.get(), frame);
            
            // Send ACK
            sendGenericAck(ctx, frame);
            
        } catch (Exception e) {
            logger.error("💥 Error handling LBS from {}: {}", 
                       ctx.channel().remoteAddress(), e.getMessage(), e);
            // Still try to send ACK
            sendGenericAck(ctx, frame);
        }
    }

    /**
     * Handle vendor multi message
     */
    private void handleVendorMulti(ChannelHandlerContext ctx, MessageFrame frame, Optional<DeviceSession> sessionOpt) {
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No session found for vendor-multi message from {}", 
                      ctx.channel().remoteAddress());
            return;
        }

        try {
            // Process vendor-specific multi-record data
            telemetryService.processVendorMultiMessage(sessionOpt.get(), frame);
            
            // Send ACK
            sendGenericAck(ctx, frame);
            
        } catch (Exception e) {
            logger.error("💥 Error handling vendor-multi from {}: {}", 
                       ctx.channel().remoteAddress(), e.getMessage(), e);
            // Still try to send ACK
            sendGenericAck(ctx, frame);
        }
    }
    
    /**
     * Handle command response
     */
    private void handleCommandResponse(ChannelHandlerContext ctx, MessageFrame frame, Optional<DeviceSession> sessionOpt) {
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No session found for command response from {}", 
                      ctx.channel().remoteAddress());
            return;
        }

        try {
            logger.info("📤 Command response received from IMEI: {} (Serial: {})", 
                      sessionOpt.get().getImei() != null ? sessionOpt.get().getImei().getValue() : "unknown",
                      frame.getSerialNumber());
            
            // Could add command response processing here
            // For now, just acknowledge
            sendGenericAck(ctx, frame);
            
        } catch (Exception e) {
            logger.error("💥 Error handling command response from {}: {}", 
                       ctx.channel().remoteAddress(), e.getMessage(), e);
            sendGenericAck(ctx, frame);
        }
    }

    /**
     * Send generic ACK response
     */
    private void sendGenericAck(ChannelHandlerContext ctx, MessageFrame frame) {
        try {
            ByteBuf ack = protocolParser.buildGenericAck(frame.getProtocolNumber(), frame.getSerialNumber());
            ctx.writeAndFlush(ack);
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
        if (cause instanceof java.io.IOException) {
            logger.warn("🚫 I/O exception, closing channel: {}", ctx.channel().remoteAddress());
            ctx.close();
        }
        // For other exceptions, just continue (don't propagate)
    }
}
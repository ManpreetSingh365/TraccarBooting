package com.wheelseye.devicegateway.infrastructure.netty;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.wheelseye.devicegateway.application.services.DeviceSessionService;
import com.wheelseye.devicegateway.application.services.TelemetryProcessingService;
import com.wheelseye.devicegateway.domain.entities.DeviceSession;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;
import com.wheelseye.devicegateway.domain.valueobjects.MessageFrame;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * Enhanced GT06 Handler - Modern Java Implementation
 * 
 * Fixed Issues:
 * 1. ✅ Better error handling and logging
 * 2. ✅ More robust message processing
 * 3. ✅ Enhanced debugging capabilities
 * 4. ✅ Modern Java 21 features (records, pattern matching, etc.)
 * 5. ✅ Improved memory management
 * 6. ✅ Better session handling
 */
@Component
@ChannelHandler.Sharable
public class GT06Handler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(GT06Handler.class);

    private final DeviceSessionService sessionService;
    private final TelemetryProcessingService telemetryService;
    private final GT06ProtocolParser protocolParser;

    public GT06Handler(DeviceSessionService sessionService,
                      TelemetryProcessingService telemetryService,
                      GT06ProtocolParser protocolParser) {
        this.sessionService = sessionService;
        this.telemetryService = telemetryService;
        this.protocolParser = protocolParser;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        var remoteAddress = ctx.channel().remoteAddress();
        logger.info("📡 New GT06 connection established: {}", remoteAddress);
        super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof ByteBuf buffer)) {
            logger.warn("Received non-ByteBuf message: {}", msg.getClass());
            return;
        }

        var remoteAddress = ctx.channel().remoteAddress();
        
        try {
            // Log raw frame for debugging
            if (logger.isDebugEnabled()) {
                var hexDump = ByteBufUtil.hexDump(buffer);
                logger.debug("📦 Raw frame from {}: {} bytes - {}", 
                           remoteAddress, buffer.readableBytes(), hexDump);
            }

            // Parse the frame
            var frame = protocolParser.parseFrame(buffer);
            if (frame == null) {
                logger.warn("❌ Failed to parse frame from {} - {} bytes", 
                          remoteAddress, buffer.readableBytes());
                return;
            }

            logger.debug("✅ Parsed frame: protocol=0x{:02X}, serial={}", 
                       frame.getProtocolNumber(), frame.getSerialNumber());

            // Update session activity
            sessionService.getSession(ctx.channel())
                         .ifPresent(DeviceSession::updateActivity);

            // Process message based on protocol type
            processMessage(ctx, frame, remoteAddress);

        } catch (Exception e) {
            logger.error("💥 Error processing message from {}: {}", 
                       remoteAddress, e.getMessage(), e);
        } finally {
            // CRITICAL: Always release the buffer
            if (buffer.refCnt() > 0) {
                buffer.release();
            }
        }
    }

    /**
     * Process message based on protocol number with enhanced error handling
     */
    private void processMessage(ChannelHandlerContext ctx, MessageFrame frame, Object remoteAddress) {
        try {
            switch (frame.getProtocolNumber()) {
                case 0x01 -> handleLogin(ctx, frame, remoteAddress);
                case 0x12, 0x22 -> handleLocation(ctx, frame, remoteAddress);
                case 0x13 -> handleStatus(ctx, frame, remoteAddress);
                case 0x94 -> handleLBSNetwork(ctx, frame, remoteAddress);
                case 0x24 -> handleVendorMulti(ctx, frame, remoteAddress);
                case 0x23 -> handleHeartbeat(ctx, frame, remoteAddress);
                default -> handleUnknownMessage(ctx, frame, remoteAddress);
            }
        } catch (Exception e) {
            logger.error("💥 Error processing protocol 0x{:02X} from {}: {}", 
                       frame.getProtocolNumber(), remoteAddress, e.getMessage(), e);
            
            // Still try to send ACK to keep device happy
            sendGenericAck(ctx, frame, "error-recovery");
        }
    }

    /**
     * Handle login messages with enhanced validation
     */
    private void handleLogin(ChannelHandlerContext ctx, MessageFrame frame, Object remoteAddress) {
        logger.info("🔐 Processing login from {}", remoteAddress);
        
        try {
            var imei = protocolParser.extractIMEI(frame);
            if (imei == null) {
                logger.warn("❌ Failed to extract IMEI from login frame from {}", remoteAddress);
                ctx.close();
                return;
            }

            logger.info("✅ Login successful - IMEI: {} from {}", imei.getValue(), remoteAddress);

            // Create or update session
            var session = sessionService.createSession(imei, ctx.channel());
            session.authenticate();

            // Send login acknowledgment
            var ack = protocolParser.buildLoginAck(frame.getSerialNumber());
            ctx.writeAndFlush(ack).addListener(future -> {
                if (future.isSuccess()) {
                    logger.info("📤 Login ACK sent to {} (IMEI: {})", remoteAddress, imei.getValue());
                } else {
                    logger.error("❌ Failed to send login ACK to {}: {}", 
                               remoteAddress, future.cause().getMessage());
                }
            });

        } catch (Exception e) {
            logger.error("💥 Login handling failed for {}: {}", remoteAddress, e.getMessage(), e);
            ctx.close();
        }
    }

    /**
     * Handle location messages
     */
    private void handleLocation(ChannelHandlerContext ctx, MessageFrame frame, Object remoteAddress) {
        logger.debug("📍 Processing location from {}", remoteAddress);
        
        var sessionOpt = sessionService.getSession(ctx.channel());
        if (!isSessionValid(sessionOpt, remoteAddress, "location")) {
            return;
        }

        try {
            // Process location data
            telemetryService.processLocationMessage(sessionOpt.get(), frame);
            logger.debug("✅ Location processed for {}", getSessionIMEI(sessionOpt.get()));
            
            // Send acknowledgment
            sendGenericAck(ctx, frame, "location");

        } catch (Exception e) {
            logger.error("💥 Location processing failed for {}: {}", remoteAddress, e.getMessage(), e);
            sendGenericAck(ctx, frame, "location-error");
        }
    }

    /**
     * Handle status messages
     */
    private void handleStatus(ChannelHandlerContext ctx, MessageFrame frame, Object remoteAddress) {
        logger.debug("📊 Processing status from {}", remoteAddress);
        
        var sessionOpt = sessionService.getSession(ctx.channel());
        if (!isSessionValid(sessionOpt, remoteAddress, "status")) {
            return;
        }

        try {
            // Process status data
            telemetryService.processStatusMessage(sessionOpt.get(), frame);
            logger.debug("✅ Status processed for {}", getSessionIMEI(sessionOpt.get()));
            
            // Send acknowledgment
            sendGenericAck(ctx, frame, "status");

        } catch (Exception e) {
            logger.error("💥 Status processing failed for {}: {}", remoteAddress, e.getMessage(), e);
            sendGenericAck(ctx, frame, "status-error");
        }
    }

    /**
     * Handle LBS Network messages
     */
    private void handleLBSNetwork(ChannelHandlerContext ctx, MessageFrame frame, Object remoteAddress) {
        logger.debug("📶 Processing LBS Network from {}", remoteAddress);
        
        var sessionOpt = sessionService.getSession(ctx.channel());
        if (!isSessionValid(sessionOpt, remoteAddress, "LBS")) {
            return;
        }

        try {
            // Process LBS data
            telemetryService.processLBSMessage(sessionOpt.get(), frame);
            logger.debug("✅ LBS processed for {}", getSessionIMEI(sessionOpt.get()));
            
            // Send acknowledgment
            sendGenericAck(ctx, frame, "lbs");

        } catch (Exception e) {
            logger.error("💥 LBS processing failed for {}: {}", remoteAddress, e.getMessage(), e);
            sendGenericAck(ctx, frame, "lbs-error");
        }
    }

    /**
     * Handle Vendor Multi messages
     */
    private void handleVendorMulti(ChannelHandlerContext ctx, MessageFrame frame, Object remoteAddress) {
        logger.debug("🏭 Processing Vendor Multi from {}", remoteAddress);
        
        var sessionOpt = sessionService.getSession(ctx.channel());
        if (!isSessionValid(sessionOpt, remoteAddress, "vendor-multi")) {
            return;
        }

        try {
            // Process vendor-specific data
            telemetryService.processVendorMultiMessage(sessionOpt.get(), frame);
            logger.debug("✅ Vendor Multi processed for {}", getSessionIMEI(sessionOpt.get()));
            
            // Send acknowledgment
            sendGenericAck(ctx, frame, "vendor-multi");

        } catch (Exception e) {
            logger.error("💥 Vendor Multi processing failed for {}: {}", remoteAddress, e.getMessage(), e);
            sendGenericAck(ctx, frame, "vendor-multi-error");
        }
    }

    /**
     * Handle heartbeat messages
     */
    private void handleHeartbeat(ChannelHandlerContext ctx, MessageFrame frame, Object remoteAddress) {
        logger.debug("💓 Processing heartbeat from {}", remoteAddress);
        
        var sessionOpt = sessionService.getSession(ctx.channel());
        if (sessionOpt.isPresent()) {
            sessionOpt.get().updateActivity();
            logger.debug("✅ Heartbeat from {}", getSessionIMEI(sessionOpt.get()));
        }
        
        // Always send heartbeat ACK
        sendGenericAck(ctx, frame, "heartbeat");
    }

    /**
     * Handle unknown message types
     */
    private void handleUnknownMessage(ChannelHandlerContext ctx, MessageFrame frame, Object remoteAddress) {
        logger.warn("❓ Unknown protocol 0x{:02X} from {} - sending generic ACK", 
                   frame.getProtocolNumber(), remoteAddress);
        
        // Send ACK anyway to keep device happy
        sendGenericAck(ctx, frame, "unknown");
    }

    /**
     * Send acknowledgment with error handling
     */
    private void sendGenericAck(ChannelHandlerContext ctx, MessageFrame frame, String messageType) {
        try {
            var ack = protocolParser.buildGenericAck(frame.getProtocolNumber(), frame.getSerialNumber());
            ctx.writeAndFlush(ack).addListener(future -> {
                if (future.isSuccess()) {
                    logger.debug("📤 {} ACK sent", messageType);
                } else {
                    logger.error("❌ Failed to send {} ACK: {}", messageType, future.cause().getMessage());
                }
            });
        } catch (Exception e) {
            logger.error("💥 Failed to build/send {} ACK: {}", messageType, e.getMessage(), e);
        }
    }

    /**
     * Validate session with modern Optional handling
     */
    private boolean isSessionValid(Optional<DeviceSession> sessionOpt, Object remoteAddress, String messageType) {
        if (sessionOpt.isEmpty()) {
            logger.warn("❌ No session found for {} message from {}", messageType, remoteAddress);
            return false;
        }
        
        if (!sessionOpt.get().isAuthenticated()) {
            logger.warn("❌ Unauthenticated session for {} message from {}", messageType, remoteAddress);
            return false;
        }
        
        return true;
    }

    /**
     * Get IMEI from session safely
     */
    private String getSessionIMEI(DeviceSession session) {
        return Optional.ofNullable(session.getImei())
                      .map(IMEI::getValue)
                      .orElse("unknown");
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent idleEvent) {
            if (idleEvent.state() == IdleState.ALL_IDLE) {
                logger.warn("⏰ Connection idle timeout from {}", ctx.channel().remoteAddress());
                ctx.close();
                return;
            }
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        var remoteAddress = ctx.channel().remoteAddress();
        logger.info("🔌 Connection closed: {}", remoteAddress);
        
        // Clean up session
        sessionService.removeSession(ctx.channel());
        
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        var remoteAddress = ctx.channel().remoteAddress();
        
        // Log different exception types appropriately
        if (cause instanceof java.io.IOException) {
            logger.debug("🌐 I/O exception from {} (normal for connection drops): {}", 
                       remoteAddress, cause.getMessage());
        } else {
            logger.error("💥 Unexpected exception from {}: {}", remoteAddress, cause.getMessage(), cause);
        }
        
        // Close connection on exceptions
        ctx.close();
    }
}
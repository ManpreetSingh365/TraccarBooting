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
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

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
    public void channelActive(ChannelHandlerContext ctx) {
        String remoteAddress = ctx.channel().remoteAddress().toString();
        logger.info("[channelActive] New connection: {}", remoteAddress);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof ByteBuf)) {
            return;
        }

        ByteBuf buffer = (ByteBuf) msg;
        try {
            Optional<DeviceSession> sessionOpt = sessionService.getSession(ctx.channel());
            MessageFrame frame = protocolParser.parseFrame(buffer);
            
            if (frame == null) {
                logger.warn("Failed to parse frame from {}", ctx.channel().remoteAddress());
                return;
            }

            // Update session activity
            sessionOpt.ifPresent(session -> session.updateActivity());

            // Process message based on type
            switch (frame.getProtocolNumber()) {
                case 0x01: // Login
                    handleLogin(ctx, frame);
                    break;
                case 0x12: // Location
                case 0x22: // GPS Location  
                    handleLocation(ctx, frame);
                    break;
                case 0x13: // Status
                    handleStatus(ctx, frame);
                    break;
                case 0x94: // LBS Network
                    handleLBSNetwork(ctx, frame);
                    break;
                case 0x24: // Vendor Multi
                    handleVendorMulti(ctx, frame);
                    break;
                default:
                    logger.warn("Unknown protocol number: 0x{:02X}", frame.getProtocolNumber());
            }

        } catch (Exception e) {
            logger.error("Error processing message from {}", ctx.channel().remoteAddress(), e);
        } finally {
            buffer.release();
        }
    }

    private void handleLogin(ChannelHandlerContext ctx, MessageFrame frame) {
        try {
            IMEI imei = protocolParser.extractIMEI(frame);
            if (imei == null) {
                logger.warn("Failed to extract IMEI from login frame");
                ctx.close();
                return;
            }

            logger.info("Login from IMEI: {}", imei.getValue());
            
            // Create or update session
            DeviceSession session = sessionService.createSession(imei, ctx.channel());
            session.authenticate();
            
            // Send login ACK
            ByteBuf ack = protocolParser.buildLoginAck(frame.getSerialNumber());
            ctx.writeAndFlush(ack);
            
            logger.info("Login ACK sent for IMEI: {}", imei.getValue());

        } catch (Exception e) {
            logger.error("Error handling login", e);
            ctx.close();
        }
    }

    private void handleLocation(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = sessionService.getSession(ctx.channel());
        if (!sessionOpt.isPresent() || !sessionOpt.get().isAuthenticated()) {
            logger.warn("Received location from unauthenticated session");
            return;
        }

        // Process location data
        telemetryService.processLocationMessage(sessionOpt.get(), frame);
        
        // Send ACK
        ByteBuf ack = protocolParser.buildGenericAck(frame.getProtocolNumber(), frame.getSerialNumber());
        ctx.writeAndFlush(ack);
    }

    private void handleStatus(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = sessionService.getSession(ctx.channel());
        if (!sessionOpt.isPresent()) {
            return;
        }

        // Process status data
        telemetryService.processStatusMessage(sessionOpt.get(), frame);
        
        // Send ACK
        ByteBuf ack = protocolParser.buildGenericAck(frame.getProtocolNumber(), frame.getSerialNumber());
        ctx.writeAndFlush(ack);
    }

    private void handleLBSNetwork(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = sessionService.getSession(ctx.channel());
        if (!sessionOpt.isPresent()) {
            return;
        }

        // Process LBS data
        telemetryService.processLBSMessage(sessionOpt.get(), frame);
        
        // Send ACK
        ByteBuf ack = protocolParser.buildGenericAck(frame.getProtocolNumber(), frame.getSerialNumber());
        ctx.writeAndFlush(ack);
    }

    private void handleVendorMulti(ChannelHandlerContext ctx, MessageFrame frame) {
        Optional<DeviceSession> sessionOpt = sessionService.getSession(ctx.channel());
        if (!sessionOpt.isPresent()) {
            return;
        }

        // Process vendor-specific multi-record data
        telemetryService.processVendorMultiMessage(sessionOpt.get(), frame);
        
        // Send ACK
        ByteBuf ack = protocolParser.buildGenericAck(frame.getProtocolNumber(), frame.getSerialNumber());
        ctx.writeAndFlush(ack);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent event = (IdleStateEvent) evt;
            if (event.state() == IdleState.ALL_IDLE) {
                logger.warn("Connection idle timeout, closing: {}", ctx.channel().remoteAddress());
                ctx.close();
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        logger.info("Connection closed: {}", ctx.channel().remoteAddress());
        sessionService.removeSession(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Exception in channel: {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}

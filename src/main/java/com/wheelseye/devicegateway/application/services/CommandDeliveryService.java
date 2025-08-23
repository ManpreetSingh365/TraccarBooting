package com.wheelseye.devicegateway.application.services;

import com.wheelseye.devicegateway.application.ports.SessionRepository;
import com.wheelseye.devicegateway.domain.entities.DeviceSession;
import com.wheelseye.devicegateway.domain.events.CommandEvent;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;
import com.wheelseye.devicegateway.infrastructure.netty.GT06ProtocolParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;

@Service
public class CommandDeliveryService {
    
    private static final Logger logger = LoggerFactory.getLogger(CommandDeliveryService.class);
    
    private final SessionRepository sessionRepository;
    private final GT06ProtocolParser protocolParser;
    
    public CommandDeliveryService(SessionRepository sessionRepository, GT06ProtocolParser protocolParser) {
        this.sessionRepository = sessionRepository;
        this.protocolParser = protocolParser;
    }
    
    public void deliverCommand(CommandEvent command) {
        IMEI imei = command.getImei();
        Optional<DeviceSession> sessionOpt = sessionRepository.findByImei(imei);
        
        if (!sessionOpt.isPresent()) {
            logger.warn("No active session found for IMEI: {} to deliver command: {}", 
                       imei.getValue(), command.getCommand());
            return;
        }
        
        DeviceSession session = sessionOpt.get();
        
        if (!session.getChannel().isActive()) {
            logger.warn("Channel not active for IMEI: {} to deliver command: {}", 
                       imei.getValue(), command.getCommand());
            return;
        }
        
        try {
            ByteBuf commandFrame = buildCommandFrame(command);
            
            session.getChannel().writeAndFlush(commandFrame).addListener(future -> {
                if (future.isSuccess()) {
                    logger.info("Successfully delivered command {} to IMEI: {}", 
                               command.getCommand(), imei.getValue());
                } else {
                    logger.error("Failed to deliver command {} to IMEI: {}", 
                               command.getCommand(), imei.getValue(), future.cause());
                }
            });
            
        } catch (Exception e) {
            logger.error("Error building command frame for command {} to IMEI: {}", 
                        command.getCommand(), imei.getValue(), e);
        }
    }
    
    private ByteBuf buildCommandFrame(CommandEvent command) {
        // Build command frame based on command type
        switch (command.getCommand().toUpperCase()) {
            case "IMMOBILIZE":
                return buildImmobilizeCommand(command);
            case "SIREN":
                return buildSirenCommand(command);
            case "LOCATE":
                return buildLocateCommand(command);
            default:
                return buildGenericCommand(command);
        }
    }
    
    private ByteBuf buildImmobilizeCommand(CommandEvent command) {
        // GT06 immobilizer command format
        ByteBuf frame = Unpooled.buffer(20);
        
        // Start bits
        frame.writeShort(0x7878);
        
        // Length (to be filled)
        int lengthIndex = frame.writerIndex();
        frame.writeByte(0x00);
        
        // Protocol number for command
        frame.writeByte(0x80);
        
        // Command content
        String action = (String) command.getParameters().getOrDefault("action", "enable");
        String commandStr = action.equals("enable") ? "DYD#" : "HFYD#";
        frame.writeBytes(commandStr.getBytes());
        
        // Serial number
        frame.writeShort(0x0001);
        
        // Update length
        int contentLength = frame.writerIndex() - lengthIndex - 1;
        frame.setByte(lengthIndex, contentLength + 2); // +2 for CRC
        
        // Calculate and write CRC
        int crc = calculateSimpleCrc(frame, 2, contentLength + 1);
        frame.writeShort(crc);
        
        // Stop bits
        frame.writeShort(0x0D0A);
        
        return frame;
    }
    
    private ByteBuf buildSirenCommand(CommandEvent command) {
        ByteBuf frame = Unpooled.buffer(20);
        
        // Start bits
        frame.writeShort(0x7878);
        
        // Length
        frame.writeByte(0x0C);
        
        // Protocol number
        frame.writeByte(0x80);
        
        // Siren command
        Map<String, Object> params = command.getParameters();
        boolean enable = (Boolean) params.getOrDefault("enable", true);
        String sirenCmd = enable ? "DXDY#" : "QXDY#";
        frame.writeBytes(sirenCmd.getBytes());
        
        // Serial number
        frame.writeShort(0x0001);
        
        // CRC
        int crc = calculateSimpleCrc(frame, 2, 7);
        frame.writeShort(crc);
        
        // Stop bits
        frame.writeShort(0x0D0A);
        
        return frame;
    }
    
    private ByteBuf buildLocateCommand(CommandEvent command) {
        ByteBuf frame = Unpooled.buffer(15);
        
        // Start bits
        frame.writeShort(0x7878);
        
        // Length
        frame.writeByte(0x05);
        
        // Protocol number for location request
        frame.writeByte(0x8A);
        
        // Serial number
        frame.writeShort(0x0001);
        
        // CRC
        int crc = calculateSimpleCrc(frame, 2, 3);
        frame.writeShort(crc);
        
        // Stop bits
        frame.writeShort(0x0D0A);
        
        return frame;
    }
    
    private ByteBuf buildGenericCommand(CommandEvent command) {
        // Generic command builder
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
        frame.writeShort(0x0001);
        
        // Update length
        int contentLength = frame.writerIndex() - lengthIndex - 1;
        frame.setByte(lengthIndex, contentLength + 2);
        
        // CRC
        int crc = calculateSimpleCrc(frame, 2, contentLength + 1);
        frame.writeShort(crc);
        
        // Stop bits
        frame.writeShort(0x0D0A);
        
        return frame;
    }
    
    private int calculateSimpleCrc(ByteBuf buffer, int start, int length) {
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
}

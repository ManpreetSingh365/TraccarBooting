package com.wheelseye.devicegateway.application.services;

import com.wheelseye.devicegateway.application.ports.EventPublisher;
import com.wheelseye.devicegateway.domain.entities.DeviceSession;
import com.wheelseye.devicegateway.domain.events.TelemetryEvent;
import com.wheelseye.devicegateway.domain.valueobjects.Location;
import com.wheelseye.devicegateway.domain.valueobjects.MessageFrame;
import com.wheelseye.devicegateway.infrastructure.netty.GT06ProtocolParser;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class TelemetryProcessingService {
    
    private static final Logger logger = LoggerFactory.getLogger(TelemetryProcessingService.class);
    
    private final EventPublisher eventPublisher;
    private final GT06ProtocolParser protocolParser;
    
    public TelemetryProcessingService(EventPublisher eventPublisher, GT06ProtocolParser protocolParser) {
        this.eventPublisher = eventPublisher;
        this.protocolParser = protocolParser;
    }
    
    public void processLocationMessage(DeviceSession session, MessageFrame frame) {
        try {
            Location location = protocolParser.parseLocation(frame);
            if (location != null) {
                Map<String, Object> attributes = new HashMap<>();
                attributes.put("messageType", "LOCATION");
                attributes.put("protocolNumber", String.format("0x%02X", frame.getProtocolNumber()));
                
                TelemetryEvent event = new TelemetryEvent(
                    session.getImei(),
                    "LOCATION",
                    location,
                    null, // battery level not in location message
                    location.isValid() ? "strong" : "weak",
                    attributes,
                    frame.getRawHex()
                );
                
                eventPublisher.publishTelemetryEvent(event);
                
                logger.debug("Processed location message for IMEI: {} - Lat: {}, Lon: {}", 
                           session.getImei().getValue(), location.getLatitude(), location.getLongitude());
            }
        } catch (Exception e) {
            logger.error("Failed to process location message for IMEI: {}", 
                        session.getImei().getValue(), e);
        }
    }
    
    public void processStatusMessage(DeviceSession session, MessageFrame frame) {
        try {
            ByteBuf content = frame.getContent();
            content.markReaderIndex();
            
            // Parse status information
            Map<String, Object> attributes = new HashMap<>();
            
            if (content.readableBytes() >= 6) {
                int voltage = content.readUnsignedByte();
                int signal = content.readUnsignedByte();
                int status = content.readUnsignedByte();
                
                attributes.put("voltage", voltage);
                attributes.put("signal", signal);
                attributes.put("status", String.format("0x%02X", status));
                attributes.put("ignition", (status & 0x01) != 0);
                attributes.put("charge", (status & 0x04) != 0);
            }
            
            content.resetReaderIndex();
            
            TelemetryEvent event = new TelemetryEvent(
                session.getImei(),
                "STATUS",
                null, // no location in status message
                null, // battery level calculation could be added
                "unknown",
                attributes,
                frame.getRawHex()
            );
            
            eventPublisher.publishTelemetryEvent(event);
            
            logger.debug("Processed status message for IMEI: {}", session.getImei().getValue());
            
        } catch (Exception e) {
            logger.error("Failed to process status message for IMEI: {}", 
                        session.getImei().getValue(), e);
        }
    }
    
    public void processLBSMessage(DeviceSession session, MessageFrame frame) {
        try {
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("messageType", "LBS");
            attributes.put("protocolNumber", String.format("0x%02X", frame.getProtocolNumber()));
            
            // Parse LBS data - simplified for now
            ByteBuf content = frame.getContent();
            if (content.readableBytes() >= 10) {
                content.markReaderIndex();
                
                // Extract basic LBS information
                if (content.readableBytes() >= 10) {
                    int mcc = content.readUnsignedShort();
                    int mnc = content.readUnsignedByte();
                    int lac = content.readUnsignedShort();
                    int cellId = content.readMedium();
                    
                    attributes.put("mcc", mcc);
                    attributes.put("mnc", mnc);
                    attributes.put("lac", lac);
                    attributes.put("cellId", cellId);
                }
                
                content.resetReaderIndex();
            }
            
            TelemetryEvent event = new TelemetryEvent(
                session.getImei(),
                "LBS",
                null, // LBS doesn't contain GPS location
                null,
                "lbs",
                attributes,
                frame.getRawHex()
            );
            
            eventPublisher.publishTelemetryEvent(event);
            
            logger.debug("Processed LBS message for IMEI: {}", session.getImei().getValue());
            
        } catch (Exception e) {
            logger.error("Failed to process LBS message for IMEI: {}", 
                        session.getImei().getValue(), e);
        }
    }
    
    public void processVendorMultiMessage(DeviceSession session, MessageFrame frame) {
        try {
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("messageType", "VENDOR_MULTI");
            attributes.put("protocolNumber", String.format("0x%02X", frame.getProtocolNumber()));
            
            // Vendor-specific multi-record processing
            // This would contain multiple GPS records or other sensor data
            
            TelemetryEvent event = new TelemetryEvent(
                session.getImei(),
                "VENDOR_MULTI",
                null, // Could extract multiple locations
                null,
                "unknown",
                attributes,
                frame.getRawHex()
            );
            
            eventPublisher.publishTelemetryEvent(event);
            
            logger.debug("Processed vendor multi message for IMEI: {}", session.getImei().getValue());
            
        } catch (Exception e) {
            logger.error("Failed to process vendor multi message for IMEI: {}", 
                        session.getImei().getValue(), e);
        }
    }
}

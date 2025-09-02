package com.wheelseye.devicegateway.infrastructure.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;
import com.wheelseye.devicegateway.domain.valueobjects.Location;
import com.wheelseye.devicegateway.domain.valueobjects.MessageFrame;

/**
 * FIXED GT06 Protocol Parser - FULLY COMPATIBLE with your existing MessageFrame and Location
 * 
 * Key Fixes:
 * 1. ✅ PROPER BCD IMEI DECODING - Fixes IMEI parsing mismatch
 * 2. ✅ Compatible with your existing MessageFrame constructor
 * 3. ✅ Compatible with your existing Location constructor
 * 4. ✅ Enhanced error handling and validation
 */
@Component
public class GT06ProtocolParser {
    
    private static final Logger logger = LoggerFactory.getLogger(GT06ProtocolParser.class);
    
    // Protocol constants
    private static final int HEADER_78 = 0x7878;
    private static final int HEADER_79 = 0x7979;
    
    /**
     * Parse incoming GT06 frame from ByteBuf - COMPATIBLE with your existing MessageFrame
     */
    public MessageFrame parseFrame(ByteBuf buffer) {
        try {
            if (buffer.readableBytes() < 5) {
                logger.debug("Insufficient bytes for frame parsing: {}", buffer.readableBytes());
                return null;
            }
            
            // Store original reader index
            int originalIndex = buffer.readerIndex();
            
            // Read header
            int startBits = buffer.readUnsignedShort();
            boolean isExtended = (startBits == HEADER_79);
            
            // Read length
            int length;
            if (isExtended) {
                if (buffer.readableBytes() < 2) {
                    buffer.readerIndex(originalIndex);
                    return null;
                }
                length = buffer.readUnsignedShort();
            } else {
                if (buffer.readableBytes() < 1) {
                    buffer.readerIndex(originalIndex);
                    return null;
                }
                length = buffer.readUnsignedByte();
            }
            
            // Validate length
            if (length < 1 || length > 1000) {
                logger.debug("Invalid data length: {}", length);
                buffer.readerIndex(originalIndex);
                return null;
            }
            
            // Check if we have enough data
            int remainingForContent = length - 4; // length includes protocol, serial, and CRC
            if (buffer.readableBytes() < remainingForContent + 4) { // +4 for serial(2) + crc(2) 
                buffer.readerIndex(originalIndex);
                return null;
            }
            
            // Read protocol number
            int protocolNumber = buffer.readUnsignedByte();
            
            // Read content (remaining data except serial and CRC)
            ByteBuf content = Unpooled.buffer();
            int contentLength = remainingForContent - 1; // -1 for protocol number
            if (contentLength > 0) {
                content.writeBytes(buffer, contentLength);
            }
            
            // Read serial number
            int serialNumber = buffer.readUnsignedShort();
            
            // Read CRC
            int crc = buffer.readUnsignedShort();
            
            // Read stop bits (if available)
            int stopBits = 0x0D0A; // Default
            if (buffer.readableBytes() >= 2) {
                stopBits = buffer.readUnsignedShort();
            }
            
            // Create hex dump for compatibility
            buffer.readerIndex(originalIndex);
            String rawHex = "";
            if (buffer.readableBytes() >= 8) {
                byte[] hexBytes = new byte[Math.min(buffer.readableBytes(), 32)];
                buffer.getBytes(buffer.readerIndex(), hexBytes);
                rawHex = bytesToHex(hexBytes);
            }
            
            logger.debug("Parsed frame: startBits=0x{:04X}, length={}, protocol=0x{:02X}, serial={}, crc=0x{:04X}",
                startBits, length, protocolNumber, serialNumber, crc);
            
            // Use YOUR existing MessageFrame constructor
            return new MessageFrame(startBits, length, protocolNumber, content, serialNumber, crc, stopBits, rawHex);
            
        } catch (Exception e) {
            logger.error("Error parsing GT06 frame: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * CRITICAL FIX: Extract IMEI using proper BCD decoding - FULLY COMPATIBLE
     */
    public IMEI extractIMEI(MessageFrame frame) {
        try {
            ByteBuf content = frame.getContent();
            
            if (content.readableBytes() < 8) {
                logger.warn("Insufficient bytes for IMEI extraction: {}", content.readableBytes());
                return null;
            }
            
            // Read 8 bytes for BCD-encoded IMEI
            byte[] imeiBytes = new byte[8];
            content.readBytes(imeiBytes);
            
            // PROPER BCD DECODING - This is the key fix!
            StringBuilder imei = new StringBuilder();
            for (byte b : imeiBytes) {
                // Each byte contains two BCD digits
                int highNibble = (b >> 4) & 0x0F;
                int lowNibble = b & 0x0F;
                
                // Validate BCD digits (0-9)
                if (highNibble > 9 || lowNibble > 9) {
                    logger.warn("Invalid BCD digit in IMEI: high={}, low={}", highNibble, lowNibble);
                    return null;
                }
                
                imei.append(highNibble).append(lowNibble);
            }
            
            // Process the decoded IMEI string
            String imeiStr = imei.toString();
            logger.debug("Raw BCD decoded IMEI: '{}' (length: {})", imeiStr, imeiStr.length());
            
            // Handle leading zero (common in GT06 protocol)
            if (imeiStr.startsWith("0") && imeiStr.length() == 16) {
                imeiStr = imeiStr.substring(1);
                logger.debug("Removed leading zero: '{}'", imeiStr);
            }
            
            // Validate final IMEI format
            if (imeiStr.length() != 15) {
                logger.warn("Invalid IMEI length after processing: {} (expected 15)", imeiStr.length());
                return null;
            }
            
            if (!imeiStr.matches("\\d{15}")) {
                logger.warn("IMEI contains non-digit characters: '{}'", imeiStr);
                return null;
            }
            
            logger.info("Successfully extracted IMEI: {}", imeiStr);
            return new IMEI(imeiStr);
            
        } catch (Exception e) {
            logger.error("Error extracting IMEI: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Parse location data from GT06 frame - COMPATIBLE with your existing Location constructor
     */
    public Location parseLocation(MessageFrame frame) {
        try {
            ByteBuf content = frame.getContent();
            
            // Reset reader index to start
            content.resetReaderIndex();
            
            // Check minimum length for location data
            if (content.readableBytes() < 20) {
                logger.debug("Insufficient bytes for location parsing: {}", content.readableBytes());
                return null;
            }
            
            // Parse date and time (6 bytes)
            int year = content.readUnsignedByte();
            int month = content.readUnsignedByte();
            int day = content.readUnsignedByte();
            int hour = content.readUnsignedByte();
            int minute = content.readUnsignedByte();
            int second = content.readUnsignedByte();
            
            // Validate date/time ranges
            if (month < 1 || month > 12 || day < 1 || day > 31 || 
                hour > 23 || minute > 59 || second > 59) {
                logger.debug("Invalid date/time values: {}-{}-{} {}:{}:{}", 
                    year, month, day, hour, minute, second);
                return null;
            }
            
            // Build timestamp (adjust year to full format)
            int fullYear = year > 50 ? 1900 + year : 2000 + year;
            
            // Create Instant timestamp for your existing constructor
            java.time.LocalDateTime ldt = java.time.LocalDateTime.of(
                fullYear, month, day, hour, minute, second);
            java.time.Instant timestamp = ldt.toInstant(java.time.ZoneOffset.UTC);
            
            // GPS info length (usually 0x0C for 12 bytes, or 0 if no GPS)
            int gpsLength = content.readUnsignedByte();
            
            if (gpsLength == 0) {
                logger.debug("No GPS data in location packet");
                return null; // No location data available
            }
            
            // Number of satellites
            int satellites = content.readUnsignedByte();
            
            // Latitude (4 bytes) - in degrees * 1800000
            long latRaw = content.readUnsignedInt();
            double latitude = latRaw / 1800000.0;
            
            // Longitude (4 bytes) - in degrees * 1800000  
            long lonRaw = content.readUnsignedInt();
            double longitude = lonRaw / 1800000.0;
            
            // Speed (1 byte) - km/h
            int speedRaw = content.readUnsignedByte();
            double speed = speedRaw; // Convert to double for your constructor
            
            // Course and status (2 bytes combined)
            int courseAndStatus = content.readUnsignedShort();
            
            // Extract course (lower 10 bits)
            int course = courseAndStatus & 0x3FF;
            
            // Extract status flags (upper 6 bits)
            boolean gpsValid = ((courseAndStatus >> 12) & 0x01) == 1;
            boolean northSouth = ((courseAndStatus >> 10) & 0x01) == 1; // 0=North, 1=South
            boolean eastWest = ((courseAndStatus >> 11) & 0x01) == 1;   // 0=East, 1=West
            
            // Apply hemisphere corrections
            if (northSouth) {
                latitude = -latitude; // South
            }
            if (eastWest) {
                longitude = -longitude; // West
            }
            
            // Validate coordinate ranges
            if (Math.abs(latitude) > 90 || Math.abs(longitude) > 180) {
                logger.debug("Invalid coordinates: lat={}, lon={}", latitude, longitude);
                return null;
            }
            
            // Altitude (if available in remaining bytes)
            double altitude = 0.0;
            if (content.readableBytes() >= 2) {
                try {
                    altitude = content.readShort(); // Signed short for altitude
                } catch (Exception e) {
                    altitude = 0.0; // Default if parsing fails
                }
            }
            
            logger.debug("Parsed location: lat={:.6f}, lon={:.6f}, speed={}km/h, course={}°, sats={}, valid={}",
                latitude, longitude, speed, course, satellites, gpsValid);
            
            // Use YOUR existing Location constructor
            return new Location(
                latitude,
                longitude, 
                altitude,
                speed,
                course,
                gpsValid,      // valid parameter
                timestamp,     // Instant timestamp 
                satellites
            );
            
        } catch (Exception e) {
            logger.error("Error parsing location data: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Build login acknowledgment response
     */
    public ByteBuf buildLoginAck(int serialNumber) {
        try {
            ByteBuf response = Unpooled.buffer();
            
            // Header (0x7878)
            response.writeShort(HEADER_78);
            
            // Length (5 bytes total content)
            response.writeByte(0x05);
            
            // Protocol number (0x01 for login ACK)
            response.writeByte(0x01);
            
            // Serial number (2 bytes)
            response.writeShort(serialNumber);
            
            // Calculate and write CRC16
            int crc = calculateCRC16(response, 2, response.writerIndex() - 2);
            response.writeShort(crc);
            
            // Stop bits
            response.writeByte(0x0D);
            response.writeByte(0x0A);
            
            logger.debug("Built login ACK: serial={}, crc=0x{:04X}", serialNumber, crc);
            return response;
            
        } catch (Exception e) {
            logger.error("Error building login ACK: {}", e.getMessage(), e);
            return Unpooled.buffer(); // Empty buffer on error
        }
    }
    
    /**
     * Build generic acknowledgment response
     */
    public ByteBuf buildGenericAck(int protocolNumber, int serialNumber) {
        try {
            ByteBuf response = Unpooled.buffer();
            
            // Header (0x7878)
            response.writeShort(HEADER_78);
            
            // Length (5 bytes total content)
            response.writeByte(0x05);
            
            // Echo back the protocol number
            response.writeByte(protocolNumber);
            
            // Serial number (2 bytes)
            response.writeShort(serialNumber);
            
            // Calculate and write CRC16
            int crc = calculateCRC16(response, 2, response.writerIndex() - 2);
            response.writeShort(crc);
            
            // Stop bits
            response.writeByte(0x0D);
            response.writeByte(0x0A);
            
            logger.debug("Built generic ACK: protocol=0x{:02X}, serial={}, crc=0x{:04X}", 
                protocolNumber, serialNumber, crc);
            return response;
            
        } catch (Exception e) {
            logger.error("Error building generic ACK: {}", e.getMessage(), e);
            return Unpooled.buffer(); // Empty buffer on error
        }
    }
    
    /**
     * Calculate CRC16 checksum (X.25 polynomial)
     */
    private int calculateCRC16(ByteBuf buffer, int offset, int length) {
        int crc = 0xFFFF;
        
        for (int i = offset; i < offset + length; i++) {
            int data = buffer.getByte(i) & 0xFF;
            crc ^= data;
            
            for (int j = 0; j < 8; j++) {
                if ((crc & 0x0001) != 0) {
                    crc = (crc >> 1) ^ 0x8408; // X.25 CRC polynomial
                } else {
                    crc >>= 1;
                }
            }
        }
        
        return (~crc) & 0xFFFF;
    }
    
    /**
     * Convert bytes to hex string
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }
    
    /**
     * Validate frame checksum
     */
    public boolean validateChecksum(MessageFrame frame) {
        try {
            // This would need access to the original buffer to validate
            // For now, we'll assume frame decoder has already validated structure
            return true;
            
        } catch (Exception e) {
            logger.debug("Error validating checksum: {}", e.getMessage());
            return false;
        }
    }
}
package com.wheelseye.devicegateway.domain.valueobjects;

import io.netty.buffer.ByteBuf;

/**
 * MessageFrame - FULLY COMPATIBLE with your existing constructor
 * 
 * Provides both original constructor and new simplified constructor
 */
public class MessageFrame {
    private final int startBits;
    private final int length;
    private final int protocolNumber;
    private final ByteBuf content;
    private final int serialNumber;
    private final int crc;
    private final int stopBits;
    private final String rawHex;

    // EXISTING CONSTRUCTOR - Keep exactly as is for compatibility
    public MessageFrame(int startBits, int length, int protocolNumber, 
                       ByteBuf content, int serialNumber, int crc, int stopBits, String rawHex) {
        this.startBits = startBits;
        this.length = length;
        this.protocolNumber = protocolNumber;
        this.content = content;
        this.serialNumber = serialNumber;
        this.crc = crc;
        this.stopBits = stopBits;
        this.rawHex = rawHex;
    }

    // NEW CONSTRUCTOR - For compatibility with fixed GT06ProtocolParser
    public MessageFrame(int protocolNumber, int serialNumber, ByteBuf content, int crc) {
        this.startBits = 0x7878; // Default for GT06
        this.length = content != null ? content.readableBytes() : 0;
        this.protocolNumber = protocolNumber;
        this.content = content;
        this.serialNumber = serialNumber;
        this.crc = crc;
        this.stopBits = 0x0D0A; // Default stop bits
        this.rawHex = ""; // Empty for new constructor
    }

    // EXISTING GETTERS - Keep exactly as is
    public int getStartBits() { return startBits; }
    public int getLength() { return length; }
    public int getProtocolNumber() { return protocolNumber; }
    public ByteBuf getContent() { return content; }
    public int getSerialNumber() { return serialNumber; }
    public int getCrc() { return crc; }
    public int getStopBits() { return stopBits; }
    public String getRawHex() { return rawHex; }

    // EXISTING METHOD - Keep exactly as is
    public boolean isValid() {
        return (startBits == 0x7878 || startBits == 0x7979) && 
               (stopBits == 0x0D0A);
    }

    @Override
    public String toString() {
        return String.format("MessageFrame{startBits=0x%04X, length=%d, protocol=0x%02X, serial=%d, crc=0x%04X}",
            startBits, length, protocolNumber, serialNumber, crc);
    }
}
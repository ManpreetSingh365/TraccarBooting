package com.wheelseye.devicegateway.domain.valueobjects;

import io.netty.buffer.ByteBuf;

public class MessageFrame {
    private final int startBits;
    private final int length;
    private final int protocolNumber;
    private final ByteBuf content;
    private final int serialNumber;
    private final int crc;
    private final int stopBits;
    private final String rawHex;

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

    public int getStartBits() { return startBits; }
    public int getLength() { return length; }
    public int getProtocolNumber() { return protocolNumber; }
    public ByteBuf getContent() { return content; }
    public int getSerialNumber() { return serialNumber; }
    public int getCrc() { return crc; }
    public int getStopBits() { return stopBits; }
    public String getRawHex() { return rawHex; }

    public boolean isValid() {
        return (startBits == 0x7878 || startBits == 0x7979) && 
               (stopBits == 0x0D0A);
    }
}

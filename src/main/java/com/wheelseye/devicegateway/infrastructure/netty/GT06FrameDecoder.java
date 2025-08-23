package com.wheelseye.devicegateway.infrastructure.netty;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class GT06FrameDecoder extends ByteToMessageDecoder {
    
    private static final Logger logger = LoggerFactory.getLogger(GT06FrameDecoder.class);
    
    private static final int MIN_FRAME_LENGTH = 5; // Start(2) + Length(1) + Protocol(1) + Stop(2) = 6, but we need at least 5 to read length
    private static final int MAX_FRAME_LENGTH = 512;
    
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        
        while (in.readableBytes() >= MIN_FRAME_LENGTH) {
            
            int readerIndex = in.readerIndex();
            
            // Look for start bits (0x7878 or 0x7979)
            int startBits = in.getUnsignedShort(readerIndex);
            
            if (startBits != 0x7878 && startBits != 0x7979) {
                // Not a valid start, skip one byte and try again
                in.readByte();
                continue;
            }
            
            // Read length field
            int lengthFieldSize = (startBits == 0x7878) ? 1 : 2;
            int totalHeaderSize = 2 + lengthFieldSize; // start bits + length field
            
            if (in.readableBytes() < totalHeaderSize) {
                // Not enough data to read length field
                break;
            }
            
            int dataLength;
            if (lengthFieldSize == 1) {
                dataLength = in.getUnsignedByte(readerIndex + 2);
            } else {
                dataLength = in.getUnsignedShort(readerIndex + 2);
            }
            
            // Calculate total frame length
            int totalFrameLength = totalHeaderSize + dataLength + 2; // +2 for stop bits
            
            if (totalFrameLength > MAX_FRAME_LENGTH) {
                logger.warn("Frame too large: {} bytes, skipping", totalFrameLength);
                in.readByte(); // Skip one byte
                continue;
            }
            
            if (in.readableBytes() < totalFrameLength) {
                // Not enough data for complete frame
                break;
            }
            
            // Verify stop bits
            int stopBitsIndex = readerIndex + totalFrameLength - 2;
            int stopBits = in.getUnsignedShort(stopBitsIndex);
            
            if (stopBits != 0x0D0A) {
                logger.warn("Invalid stop bits: 0x{:04X}, expected 0x0D0A", stopBits);
                in.readByte(); // Skip one byte
                continue;
            }
            
            // Extract complete frame
            ByteBuf frame = in.readRetainedSlice(totalFrameLength);
            out.add(frame);
            
            logger.debug("Decoded GT06 frame: {} bytes", totalFrameLength);
        }
    }
}

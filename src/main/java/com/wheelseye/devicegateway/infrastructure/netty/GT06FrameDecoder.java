package com.wheelseye.devicegateway.infrastructure.netty;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * Enhanced GT06 Frame Decoder for WhelsEye Device Gateway
 * 
 * This decoder properly handles:
 * 1. Concatenated messages in single TCP packets
 * 2. Fragmented messages across multiple TCP packets  
 * 3. Both 78 78 and 79 79 frame headers
 * 4. Variable length encoding for different frame types
 * 5. Proper memory management to prevent ByteBuf leaks
 * 6. Robust error handling and frame validation
 * 
 * Compatible with Java 21 and follows modern best practices.
 */
public class GT06FrameDecoder extends ByteToMessageDecoder {
    
    private static final Logger logger = LoggerFactory.getLogger(GT06FrameDecoder.class);
    
    // Protocol constants
    private static final int MAX_FRAME_LENGTH = 1024;
    private static final int MIN_FRAME_LENGTH = 5;
    private static final int HEADER_78_SIZE = 2; // 78 78
    private static final int HEADER_79_SIZE = 2; // 79 79
    private static final int LENGTH_FIELD_78_SIZE = 1;
    private static final int LENGTH_FIELD_79_SIZE = 2;
    private static final int CRC_SIZE = 2;
    private static final int STOP_BITS_SIZE = 2; // 0D 0A
    
    // Frame markers
    private static final int HEADER_78 = 0x7878;
    private static final int HEADER_79 = 0x7979;
    private static final int STOP_BITS = 0x0D0A;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
        
        while (buffer.readableBytes() >= MIN_FRAME_LENGTH) {
            // Mark current position for potential reset
            buffer.markReaderIndex();
            
            // Find the start of the next GT06 frame
            int frameStart = findFrameStart(buffer);
            if (frameStart == -1) {
                // No valid frame start found, wait for more data
                buffer.resetReaderIndex();
                break;
            }
            
            // Skip to frame start if not at current position
            if (frameStart > 0) {
                buffer.skipBytes(frameStart);
                logger.debug("Skipped {} bytes to reach frame start", frameStart);
            }
            
            // Ensure we have enough bytes to read header and length
            if (buffer.readableBytes() < MIN_FRAME_LENGTH) {
                buffer.resetReaderIndex();
                break;
            }
            
            // Read and validate frame header
            int header = buffer.getUnsignedShort(buffer.readerIndex());
            boolean is78Frame = (header == HEADER_78);
            boolean is79Frame = (header == HEADER_79);
            
            if (!is78Frame && !is79Frame) {
                // Invalid header, skip one byte and try again
                buffer.skipBytes(1);
                logger.warn("Invalid GT06 header: 0x{:04X}, skipping byte", header);
                continue;
            }
            
            // Calculate frame parameters based on header type
            int headerSize = is78Frame ? HEADER_78_SIZE : HEADER_79_SIZE;
            int lengthFieldSize = is78Frame ? LENGTH_FIELD_78_SIZE : LENGTH_FIELD_79_SIZE;
            int totalHeaderSize = headerSize + lengthFieldSize;
            
            // Check if we have enough bytes to read the length field
            if (buffer.readableBytes() < totalHeaderSize) {
                buffer.resetReaderIndex();
                break;
            }
            
            // Read data length from frame
            int dataLength;
            if (is78Frame) {
                dataLength = buffer.getUnsignedByte(buffer.readerIndex() + HEADER_78_SIZE);
            } else {
                int lengthOffset = buffer.readerIndex() + HEADER_79_SIZE;
                dataLength = buffer.getUnsignedShort(lengthOffset);
            }
            
            // Calculate total frame length
            int totalFrameLength = totalHeaderSize + dataLength + CRC_SIZE + STOP_BITS_SIZE;
            
            // Validate frame length bounds
            if (totalFrameLength > MAX_FRAME_LENGTH) {
                logger.warn("Frame too large: {} bytes (max: {}), skipping", totalFrameLength, MAX_FRAME_LENGTH);
                buffer.skipBytes(1);
                continue;
            }
            
            if (totalFrameLength < MIN_FRAME_LENGTH) {
                logger.warn("Frame too small: {} bytes (min: {}), skipping", totalFrameLength, MIN_FRAME_LENGTH);
                buffer.skipBytes(1);
                continue;
            }
            
            // Check if we have the complete frame
            if (buffer.readableBytes() < totalFrameLength) {
                // Not enough data for complete frame, wait for more
                buffer.resetReaderIndex();
                logger.debug("Incomplete frame: need {} bytes, have {}", totalFrameLength, buffer.readableBytes());
                break;
            }
            
            // Validate frame ending (stop bits)
            int stopBitsOffset = buffer.readerIndex() + totalFrameLength - STOP_BITS_SIZE;
            int stopBits = buffer.getUnsignedShort(stopBitsOffset);
            if (stopBits != STOP_BITS) {
                logger.warn("Invalid stop bits: 0x{:04X} (expected: 0x{:04X}), skipping frame", 
                           stopBits, STOP_BITS);
                buffer.skipBytes(1);
                continue;
            }
            
            // Extract the complete, validated frame
            try {
                ByteBuf frame = buffer.readRetainedSlice(totalFrameLength);
                out.add(frame);
                
                logger.debug("Successfully decoded GT06 frame: {} bytes, header: 0x{:04X}, protocol: 0x{:02X}", 
                           totalFrameLength, header, getProtocolNumber(frame, is78Frame));
                
            } catch (Exception e) {
                logger.error("Failed to extract frame", e);
                buffer.skipBytes(1);
                continue;
            }
        }
    }
    
    /**
     * Find the start of the next GT06 frame in the buffer
     * @param buffer The input buffer
     * @return Offset to frame start, or -1 if not found
     */
    private int findFrameStart(ByteBuf buffer) {
        int readerIndex = buffer.readerIndex();
        int searchLimit = buffer.readableBytes() - 1; // Need at least 2 bytes for header
        
        for (int i = 0; i < searchLimit; i++) {
            int currentPos = readerIndex + i;
            int header = buffer.getUnsignedShort(currentPos);
            
            if (header == HEADER_78 || header == HEADER_79) {
                logger.debug("Found GT06 frame start at offset {}: 0x{:04X}", i, header);
                return i;
            }
        }
        
        logger.debug("No GT06 frame start found in {} bytes", searchLimit + 1);
        return -1; // No frame start found
    }
    
    /**
     * Extract protocol number from frame for logging purposes
     */
    private int getProtocolNumber(ByteBuf frame, boolean is78Frame) {
        try {
            int protocolOffset = is78Frame ? 3 : 4; // After header + length field
            if (frame.readableBytes() > protocolOffset) {
                return frame.getUnsignedByte(protocolOffset);
            }
        } catch (Exception e) {
            logger.debug("Could not extract protocol number", e);
        }
        return -1;
    }
    
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Exception in GT06FrameDecoder from {}: {}", 
                    ctx.channel().remoteAddress(), cause.getMessage(), cause);
        
        // Don't close the channel for parsing errors, just continue
        // Only close for serious I/O errors
        if (cause instanceof java.io.IOException) {
            logger.warn("I/O exception, closing channel: {}", ctx.channel().remoteAddress());
            ctx.close();
        } else {
            // For other exceptions, just log and continue
            super.exceptionCaught(ctx, cause);
        }
    }
    
    @Override
    protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Process any remaining data when channel is closing
        if (in.readableBytes() > 0) {
            logger.debug("Processing remaining {} bytes on channel close", in.readableBytes());
            decode(ctx, in, out);
        }
    }
        
    @Override
    protected void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
        logger.debug("GT06FrameDecoder removed from pipeline: {}", ctx.channel().remoteAddress());
        super.handlerRemoved0(ctx);
    }
}
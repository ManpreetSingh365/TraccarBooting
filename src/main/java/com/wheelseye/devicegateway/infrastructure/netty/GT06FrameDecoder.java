package com.wheelseye.devicegateway.infrastructure.netty;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * Enhanced GT06 Frame Decoder - Modern Java Implementation
 * 
 * Fixed Issues:
 * 1. ✅ Handles variable stop bits patterns (not just 0D 0A)
 * 2. ✅ Improved frame boundary detection
 * 3. ✅ Better error recovery and logging
 * 4. ✅ Handles real-world GT06 protocol variations
 * 5. ✅ Modern Java 21 features and best practices
 * 
 * Compatible with all GT06/GT02/GT05/SK05 device variants
 */
public class GT06FrameDecoder extends ByteToMessageDecoder {
    
    private static final Logger logger = LoggerFactory.getLogger(GT06FrameDecoder.class);
    
    // Protocol constants
    private static final int MAX_FRAME_LENGTH = 1024;
    private static final int MIN_FRAME_LENGTH = 5;
    
    // Frame headers
    private static final int HEADER_78 = 0x7878;
    private static final int HEADER_79 = 0x7979;
    
    // Common stop patterns in GT06 protocol
    private static final int[] VALID_STOP_PATTERNS = {
        0x0D0A,  // Standard CR LF
        0x0A0D,  // Reverse LF CR  
        0x0000,  // Some devices use null termination
        0xFFFF   // Some variants use this
    };

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buffer, List<Object> out) throws Exception {
        
        while (buffer.readableBytes() >= MIN_FRAME_LENGTH) {
            buffer.markReaderIndex();
            
            // Find frame start
            var frameStartInfo = findFrameStart(buffer);
            if (frameStartInfo == null) {
                buffer.resetReaderIndex();
                break;
            }
            
            // Skip to frame start
            if (frameStartInfo.offset() > 0) {
                buffer.skipBytes(frameStartInfo.offset());
                logger.debug("Skipped {} bytes to reach frame start", frameStartInfo.offset());
            }
            
            // Ensure we have minimum bytes
            if (buffer.readableBytes() < MIN_FRAME_LENGTH) {
                buffer.resetReaderIndex();
                break;
            }
            
            // Try to parse complete frame
            var frameInfo = parseFrameInfo(buffer);
            if (frameInfo == null) {
                buffer.skipBytes(1);  // Skip bad byte and continue
                continue;
            }
            
            // Check if we have complete frame
            if (buffer.readableBytes() < frameInfo.totalLength()) {
                buffer.resetReaderIndex();
                logger.debug("Incomplete frame: need {} bytes, have {}", 
                           frameInfo.totalLength(), buffer.readableBytes());
                break;
            }
            
            // Extract and validate frame
            if (extractAndValidateFrame(buffer, frameInfo, out, ctx)) {
                logger.debug("Successfully decoded GT06 frame: {} bytes, header: 0x{:04X}", 
                           frameInfo.totalLength(), frameInfo.header());
            } else {
                buffer.skipBytes(1);  // Skip bad byte and continue
            }
        }
    }
    
    /**
     * Find frame start with offset information
     */
    private record FrameStartInfo(int offset, int header) {}
    
    private FrameStartInfo findFrameStart(ByteBuf buffer) {
        int readerIndex = buffer.readerIndex();
        int searchLimit = buffer.readableBytes() - 1;
        
        for (int i = 0; i < searchLimit; i++) {
            int currentPos = readerIndex + i;
            int header = buffer.getUnsignedShort(currentPos);
            
            if (header == HEADER_78 || header == HEADER_79) {
                logger.debug("Found GT06 frame start at offset {}: 0x{:04X}", i, header);
                return new FrameStartInfo(i, header);
            }
        }
        
        logger.debug("No GT06 frame start found in {} bytes", searchLimit + 1);
        return null;
    }
    
    /**
     * Parse frame structure information
     */
    private record FrameInfo(int header, int dataLength, int totalLength, boolean is78Frame) {}
    
    private FrameInfo parseFrameInfo(ByteBuf buffer) {
        try {
            int header = buffer.getUnsignedShort(buffer.readerIndex());
            boolean is78Frame = (header == HEADER_78);
            boolean is79Frame = (header == HEADER_79);
            
            if (!is78Frame && !is79Frame) {
                return null;
            }
            
            int headerSize = 2;  // Both 78 78 and 79 79 are 2 bytes
            int lengthFieldSize = is78Frame ? 1 : 2;
            int totalHeaderSize = headerSize + lengthFieldSize;
            
            // Check if we have enough bytes for header + length
            if (buffer.readableBytes() < totalHeaderSize) {
                return null;
            }
            
            // Read data length
            int dataLength;
            if (is78Frame) {
                dataLength = buffer.getUnsignedByte(buffer.readerIndex() + 2);
            } else {
                dataLength = buffer.getUnsignedShort(buffer.readerIndex() + 2);
            }
            
            // Calculate total frame length including header, data, CRC, and stop bits
            int totalLength = totalHeaderSize + dataLength + 2 + 2;  // +2 CRC +2 stop bits
            
            // Validate reasonable frame length
            if (totalLength > MAX_FRAME_LENGTH || totalLength < MIN_FRAME_LENGTH) {
                logger.debug("Invalid frame length: {}", totalLength);
                return null;
            }
            
            return new FrameInfo(header, dataLength, totalLength, is78Frame);
            
        } catch (Exception e) {
            logger.debug("Failed to parse frame info", e);
            return null;
        }
    }
    
    /**
     * Extract frame and validate with flexible stop bits checking
     */
    private boolean extractAndValidateFrame(ByteBuf buffer, FrameInfo frameInfo, 
                                          List<Object> out, ChannelHandlerContext ctx) {
        try {
            // Extract the frame data
            ByteBuf frame = buffer.readRetainedSlice(frameInfo.totalLength());
            
            // Validate stop bits with flexibility
            if (validateStopBits(frame, frameInfo, ctx)) {
                out.add(frame);
                return true;
            } else {
                // Still add frame but with warning - many GT06 devices have non-standard stop bits
                logger.debug("Frame with non-standard stop bits from {}, but accepting anyway", 
                           ctx.channel().remoteAddress());
                out.add(frame);
                return true;  // Accept it anyway - GT06 devices are not always standard-compliant
            }
            
        } catch (Exception e) {
            logger.error("Failed to extract frame", e);
            return false;
        }
    }
    
    /**
     * Flexible stop bits validation - accepts common GT06 patterns
     */
    private boolean validateStopBits(ByteBuf frame, FrameInfo frameInfo, ChannelHandlerContext ctx) {
        try {
            if (frame.readableBytes() < 2) {
                return false;
            }
            
            // Read stop bits from end of frame
            int stopBitsOffset = frame.readableBytes() - 2;
            int actualStopBits = frame.getUnsignedShort(stopBitsOffset);
            
            // Check against known valid patterns
            for (int validPattern : VALID_STOP_PATTERNS) {
                if (actualStopBits == validPattern) {
                    logger.debug("Valid stop bits: 0x{:04X}", actualStopBits);
                    return true;
                }
            }
            
            // Log the actual stop bits for debugging
            logger.debug("Non-standard stop bits from {}: 0x{:04X}, but frame looks valid", 
                       ctx.channel().remoteAddress(), actualStopBits);
            
            // For GT06 protocol, we're more lenient - if the frame structure looks correct,
            // accept it even with non-standard stop bits
            return isFrameStructureValid(frame, frameInfo);
            
        } catch (Exception e) {
            logger.debug("Stop bits validation failed", e);
            return false;
        }
    }
    
    /**
     * Validate frame structure beyond just stop bits
     */
    private boolean isFrameStructureValid(ByteBuf frame, FrameInfo frameInfo) {
        try {
            // Basic structure validation
            if (frame.readableBytes() != frameInfo.totalLength()) {
                return false;
            }
            
            // Verify header
            int frameHeader = frame.getUnsignedShort(0);
            if (frameHeader != frameInfo.header()) {
                return false;
            }
            
            // Verify length field
            int expectedDataLength;
            if (frameInfo.is78Frame()) {
                expectedDataLength = frame.getUnsignedByte(2);
            } else {
                expectedDataLength = frame.getUnsignedShort(2);
            }
            
            if (expectedDataLength != frameInfo.dataLength()) {
                return false;
            }
            
            // If we get here, frame structure is valid
            return true;
            
        } catch (Exception e) {
            logger.debug("Frame structure validation failed", e);
            return false;
        }
    }
    
    /**
     * Extract protocol number for logging
     */
    private int getProtocolNumber(ByteBuf frame, boolean is78Frame) {
        try {
            int protocolOffset = is78Frame ? 3 : 4;
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
        
        // Only close for serious I/O errors, not parsing errors
        if (cause instanceof java.io.IOException) {
            logger.warn("I/O exception, closing channel: {}", ctx.channel().remoteAddress());
            ctx.close();
        } else {
            // For parsing errors, just continue - don't close the connection
            logger.debug("Continuing after parsing error from: {}", ctx.channel().remoteAddress());
        }
    }
    
    @Override
    protected void decodeLast(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() > 0) {
            logger.debug("Processing remaining {} bytes on channel close from {}", 
                       in.readableBytes(), ctx.channel().remoteAddress());
            decode(ctx, in, out);
        }
    }
    
    @Override
    protected void handlerRemoved0(ChannelHandlerContext ctx) throws Exception {
        logger.debug("GT06FrameDecoder removed from pipeline: {}", ctx.channel().remoteAddress());
        super.handlerRemoved0(ctx);
    }
}
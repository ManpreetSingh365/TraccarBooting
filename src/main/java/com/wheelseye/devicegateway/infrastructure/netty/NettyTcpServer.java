package com.wheelseye.devicegateway.infrastructure.netty;

import com.wheelseye.devicegateway.application.services.DeviceSessionService;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;

@Component
public class NettyTcpServer {
    private static final Logger logger = LoggerFactory.getLogger(NettyTcpServer.class);

    @Value("${device-gateway.tcp.port:5023}")
    private int port;

    @Value("${device-gateway.tcp.boss-threads:1}")
    private int bossThreads;

    @Value("${device-gateway.tcp.worker-threads:4}")
    private int workerThreads;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;
    
    private final DeviceSessionService sessionService;
    private final GT06Handler gt06Handler;

    public NettyTcpServer(DeviceSessionService sessionService, GT06Handler gt06Handler) {
        this.sessionService = sessionService;
        this.gt06Handler = gt06Handler;
    }

    @PostConstruct
    public void start() throws InterruptedException {
        logger.info("Starting GT06 TCP Server on port {}", port);
        
        bossGroup = new NioEventLoopGroup(bossThreads);
        workerGroup = new NioEventLoopGroup(workerThreads);

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            
                            // Add idle state handler for connection timeout
                            pipeline.addLast(new IdleStateHandler(0, 0, 300, TimeUnit.SECONDS));
                            
                            // Add GT06 frame decoder
                            pipeline.addLast(new GT06FrameDecoder());
                            
                            // Add GT06 protocol handler
                            pipeline.addLast(gt06Handler);
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true);

            ChannelFuture future = bootstrap.bind(port).sync();
            serverChannel = future.channel();
            
            logger.info("🚀 GT06 Server started successfully on port {}", port);
            
        } catch (Exception e) {
            logger.error("Failed to start GT06 Server", e);
            shutdown();
            throw e;
        }
    }

    @PreDestroy
    public void shutdown() {
        logger.info("Shutting down GT06 TCP Server");
        
        if (serverChannel != null) {
            serverChannel.close();
        }
        
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
        }
        
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
        }
    }
}

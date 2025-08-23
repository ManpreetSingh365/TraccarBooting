package com.wheelseye.devicegateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "device-gateway.tcp")
public class NettyConfig {
    
    private int port = 5023;
    private int bossThreads = 1;
    private int workerThreads = 4;
    private int backlog = 128;
    private boolean keepAlive = true;
    private boolean tcpNoDelay = true;
    private int idleTimeoutSeconds = 600;
    
    // Getters and setters
    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }
    
    public int getBossThreads() { return bossThreads; }
    public void setBossThreads(int bossThreads) { this.bossThreads = bossThreads; }
    
    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
    
    public int getBacklog() { return backlog; }
    public void setBacklog(int backlog) { this.backlog = backlog; }
    
    public boolean isKeepAlive() { return keepAlive; }
    public void setKeepAlive(boolean keepAlive) { this.keepAlive = keepAlive; }
    
    public boolean isTcpNoDelay() { return tcpNoDelay; }
    public void setTcpNoDelay(boolean tcpNoDelay) { this.tcpNoDelay = tcpNoDelay; }
    
    public int getIdleTimeoutSeconds() { return idleTimeoutSeconds; }
    public void setIdleTimeoutSeconds(int idleTimeoutSeconds) { this.idleTimeoutSeconds = idleTimeoutSeconds; }
}

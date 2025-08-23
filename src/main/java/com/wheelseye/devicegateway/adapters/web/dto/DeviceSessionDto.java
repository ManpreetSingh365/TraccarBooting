package com.wheelseye.devicegateway.adapters.web.dto;

public class DeviceSessionDto {
    private String sessionId;
    private String imei;
    private String connectedAt;
    private String lastActivity;
    private boolean authenticated;
    private boolean active;
    
    // Default constructor for ModelMapper
    public DeviceSessionDto() {}
    
    public DeviceSessionDto(String sessionId, String imei, String connectedAt, 
                           String lastActivity, boolean authenticated, boolean active) {
        this.sessionId = sessionId;
        this.imei = imei;
        this.connectedAt = connectedAt;
        this.lastActivity = lastActivity;
        this.authenticated = authenticated;
        this.active = active;
    }
    
    // Getters and Setters
    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }
    
    public String getImei() { return imei; }
    public void setImei(String imei) { this.imei = imei; }
    
    public String getConnectedAt() { return connectedAt; }
    public void setConnectedAt(String connectedAt) { this.connectedAt = connectedAt; }
    
    public String getLastActivity() { return lastActivity; }
    public void setLastActivity(String lastActivity) { this.lastActivity = lastActivity; }
    
    public boolean isAuthenticated() { return authenticated; }
    public void setAuthenticated(boolean authenticated) { this.authenticated = authenticated; }
    
    public boolean isActive() { return active; }
    public void setActive(boolean active) { this.active = active; }
}

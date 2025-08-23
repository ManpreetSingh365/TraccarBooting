package com.wheelseye.devicegateway.domain.events;

import java.time.Instant;
import java.util.Map;

import com.wheelseye.devicegateway.domain.valueobjects.IMEI;

public class CommandEvent {
    private final String commandId;
    private final IMEI imei;
    private final String command;
    private final Map<String, Object> parameters;
    private final String priority;
    private final int retryCount;
    private final int maxRetries;
    private final Instant timestamp;

    public CommandEvent(String commandId, IMEI imei, String command, 
                       Map<String, Object> parameters, String priority, 
                       int retryCount, int maxRetries) {
        this.commandId = commandId;
        this.imei = imei;
        this.command = command;
        this.parameters = parameters;
        this.priority = priority;
        this.retryCount = retryCount;
        this.maxRetries = maxRetries;
        this.timestamp = Instant.now();
    }

    // Getters
    public String getCommandId() { return commandId; }
    public IMEI getImei() { return imei; }
    public String getCommand() { return command; }
    public Map<String, Object> getParameters() { return parameters; }
    public String getPriority() { return priority; }
    public int getRetryCount() { return retryCount; }
    public int getMaxRetries() { return maxRetries; }
    public Instant getTimestamp() { return timestamp; }
}

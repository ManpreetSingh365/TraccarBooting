package com.wheelseye.devicegateway.application.ports;

import com.wheelseye.devicegateway.domain.events.DeviceSessionEvent;
import com.wheelseye.devicegateway.domain.events.TelemetryEvent;
import com.wheelseye.devicegateway.domain.events.CommandEvent;

public interface EventPublisher {
    void publishDeviceSessionEvent(DeviceSessionEvent event);
    void publishTelemetryEvent(TelemetryEvent event);
    void publishCommandEvent(CommandEvent event);
}

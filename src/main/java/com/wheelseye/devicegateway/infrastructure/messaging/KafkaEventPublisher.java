package com.wheelseye.devicegateway.infrastructure.messaging;

import com.wheelseye.devicegateway.application.ports.EventPublisher;
import com.wheelseye.devicegateway.domain.events.CommandEvent;
import com.wheelseye.devicegateway.domain.events.DeviceSessionEvent;
import com.wheelseye.devicegateway.domain.events.TelemetryEvent;
import com.wheelseye.devicegateway.domain.mappers.TelemetryEventMapper;
import com.wheelseye.devicegateway.domain.mappers.CommandEventMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import com.wheelseye.devicegateway.domain.mappers.DeviceSessionEventMapper;

@Component
public class KafkaEventPublisher implements EventPublisher {

    private static final Logger logger = LoggerFactory.getLogger(KafkaEventPublisher.class);

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    @Value("${device-gateway.kafka.topics.device-sessions}")
    private String deviceSessionsTopic;

    @Value("${device-gateway.kafka.topics.telemetry-inbound}")
    private String telemetryInboundTopic;

    @Value("${device-gateway.kafka.topics.commands-outbound}")
    private String commandsOutboundTopic;

    public KafkaEventPublisher(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publishDeviceSessionEvent(DeviceSessionEvent event) {
        try {
            kafkaTemplate.send(deviceSessionsTopic, event.getImei().getValue(), DeviceSessionEventMapper.toProto(event).toByteArray())
                    .addCallback(
                            result -> logger.debug("Published session event for IMEI: {}", event.getImei().getValue()),
                            failure -> logger.error("Failed to publish session event for IMEI: {}",
                                    event.getImei().getValue(), failure));
        } catch (Exception e) {
            logger.error("Error publishing device session event", e);
        }
    }

    @Override
    public void publishTelemetryEvent(TelemetryEvent event) {
        try {
            kafkaTemplate.send(telemetryInboundTopic, event.getImei().getValue(), TelemetryEventMapper.toProto(event).toByteArray())
                    .addCallback(
                            result -> logger.debug("Published telemetry event for IMEI: {}",
                                    event.getImei().getValue()),
                            failure -> logger.error("Failed to publish telemetry event for IMEI: {}",
                                    event.getImei().getValue(), failure));
        } catch (Exception e) {
            logger.error("Error publishing telemetry event", e);
        }
    }

    @Override
    public void publishCommandEvent(CommandEvent event) {
        try {
            kafkaTemplate.send(commandsOutboundTopic, event.getImei().getValue(), CommandEventMapper.toProto(event).toByteArray())
                    .addCallback(
                            result -> logger.debug("Published command event for IMEI: {}", event.getImei().getValue()),
                            failure -> logger.error("Failed to publish command event for IMEI: {}",
                                    event.getImei().getValue(), failure));
        } catch (Exception e) {
            logger.error("Error publishing command event", e);
        }
    }

}

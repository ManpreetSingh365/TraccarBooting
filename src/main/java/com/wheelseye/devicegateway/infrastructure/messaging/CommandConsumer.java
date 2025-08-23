package com.wheelseye.devicegateway.infrastructure.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.wheelseye.devicegateway.application.services.CommandDeliveryService;
import com.wheelseye.devicegateway.domain.events.CommandEvent;

@Component
public class CommandConsumer {

    private static final Logger logger = LoggerFactory.getLogger(CommandConsumer.class);

    private final CommandDeliveryService commandDeliveryService;
    private final KafkaListenerEndpointRegistry registry;

    public CommandConsumer(CommandDeliveryService commandDeliveryService,
            KafkaListenerEndpointRegistry registry) {
        this.commandDeliveryService = commandDeliveryService;
        this.registry = registry;
    }

    @EventListener(ContextRefreshedEvent.class)
    public void onApplicationEvent() {
        // Start all Kafka listeners only after the application context is ready
        registry.getListenerContainer("commandListener").start();
    }

    // @KafkaListener(topics = "${device-gateway.kafka.topics.commands-outbound}",
    // groupId = "device-gateway-commands-group")
    @KafkaListener(id = "commandListener", topics = "${device-gateway.kafka.topics.commands-outbound}", groupId = "${spring.kafka.consumer.group-id}")
        public void handleCommandEvent(@Payload CommandEvent command,
            @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String imei,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        logger.info("Received command {} for IMEI: {} from topic: {} partition: {} offset: {}",
                command.getCommand(), imei, topic, partition, offset);

        try {
            commandDeliveryService.deliverCommand(command);
            logger.debug("Successfully processed command {} for IMEI: {}",
                    command.getCommand(), imei);

            // Manually acknowledge the message
            acknowledgment.acknowledge();

        } catch (Exception e) {
            logger.error("Failed to process command {} for IMEI: {}",
                    command.getCommand(), imei, e);
            // TODO: Implement dead letter queue or retry logic
            // For now, acknowledge to prevent reprocessing
            acknowledgment.acknowledge();
        }
    }
}

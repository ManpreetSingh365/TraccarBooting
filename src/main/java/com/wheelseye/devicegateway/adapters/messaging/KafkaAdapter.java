package com.wheelseye.devicegateway.adapters.messaging;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaAdapter {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public KafkaAdapter(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String topic, String key, byte[] message) {
        kafkaTemplate.send(topic, key, message);
    }
}
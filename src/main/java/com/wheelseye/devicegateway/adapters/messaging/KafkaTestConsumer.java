package com.wheelseye.devicegateway.adapters.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTestConsumer {

    @KafkaListener(topics = "device.sessions", groupId = "device-gateway-group")
    public void listen(ConsumerRecord<String, byte[]> record) {
        System.out.println("Received message:");
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + new String(record.value())); // convert bytes to string
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());
    }
}

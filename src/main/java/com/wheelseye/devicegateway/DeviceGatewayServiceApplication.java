package com.wheelseye.devicegateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableKafka
@EnableScheduling
public class DeviceGatewayServiceApplication {
    
    private static final Logger logger = LoggerFactory.getLogger(DeviceGatewayServiceApplication.class);
    
    public static void main(String[] args) {
        logger.info("🚀 Starting Device Gateway Service...");
        SpringApplication.run(DeviceGatewayServiceApplication.class, args);
        logger.info("✅ Device Gateway Service started successfully!");
    }
}

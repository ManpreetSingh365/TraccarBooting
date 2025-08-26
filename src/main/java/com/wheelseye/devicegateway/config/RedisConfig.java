package com.wheelseye.devicegateway.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Redis Configuration - Fixed for proper serialization of complex objects
 * 
 * Fixes:
 * 1. ✅ Proper ObjectMapper with JavaTimeModule for Instant support
 * 2. ✅ Handles LinkedHashMap to custom object deserialization
 * 3. ✅ Enables default typing for complex object hierarchies
 */
@Configuration
public class RedisConfig {

    @Bean
    public ObjectMapper redisObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();

        // Enable JSR310 module for Java 8 time types (Instant, LocalDateTime, etc.)
        objectMapper.registerModule(new JavaTimeModule());

        // Disable writing dates as timestamps
        objectMapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // Enable default typing to handle complex object hierarchies
        // This helps with SessionData and other custom classes
        objectMapper.activateDefaultTyping(
                objectMapper.getPolymorphicTypeValidator(),
                ObjectMapper.DefaultTyping.NON_FINAL,
                com.fasterxml.jackson.annotation.JsonTypeInfo.As.PROPERTY);

        // Configure to handle missing properties gracefully
        objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
                false);

        return objectMapper;
    }

    @Bean
    public GenericJackson2JsonRedisSerializer redisJsonSerializer() {
        return new GenericJackson2JsonRedisSerializer(redisObjectMapper());
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);

        // Use String serializer for keys
        StringRedisSerializer stringSerializer = new StringRedisSerializer();
        template.setKeySerializer(stringSerializer);
        template.setHashKeySerializer(stringSerializer);

        // Use custom JSON serializer for values
        GenericJackson2JsonRedisSerializer jsonSerializer = redisJsonSerializer();
        template.setValueSerializer(jsonSerializer);
        template.setHashValueSerializer(jsonSerializer);

        // Enable transaction support
        template.setEnableTransactionSupport(true);

        template.afterPropertiesSet();
        return template;
    }

    @Bean
    public RedisTemplate<String, String> customRedisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setDefaultSerializer(new StringRedisSerializer());
        return template;
    }
}
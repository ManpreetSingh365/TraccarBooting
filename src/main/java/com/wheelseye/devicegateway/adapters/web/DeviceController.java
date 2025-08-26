package com.wheelseye.devicegateway.adapters.web;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.modelmapper.ModelMapper;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.wheelseye.devicegateway.adapters.web.dto.DeviceSessionDto;
import com.wheelseye.devicegateway.application.services.DeviceSessionService;
import com.wheelseye.devicegateway.domain.entities.DeviceSession;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;

@RestController
@RequestMapping("/api/v1/devices")
public class DeviceController {
    
    private final DeviceSessionService deviceSessionService;
    private final ModelMapper modelMapper;
    
    public DeviceController(DeviceSessionService deviceSessionService, ModelMapper modelMapper) {
        this.deviceSessionService = deviceSessionService;
        this.modelMapper = modelMapper;
    }
    
    @GetMapping("/sessions")
    public ResponseEntity<Collection<DeviceSessionDto>> getAllSessions() {
        Collection<DeviceSession> sessions = deviceSessionService.getAllSessions();
        Collection<DeviceSessionDto> sessionDtos = sessions.stream()
            .map(session -> modelMapper.map(session, DeviceSessionDto.class))
            .collect(java.util.stream.Collectors.toList());
        
        return ResponseEntity.ok(sessionDtos);
    }
    
    @GetMapping("/{imei}/session")
    public ResponseEntity<DeviceSessionDto> getSessionByImei(@PathVariable String imei) {
        try {
            IMEI deviceImei = new IMEI(imei);
            Optional<DeviceSession> session = deviceSessionService.getSession(deviceImei);
            
            if (session.isPresent()) {
                return ResponseEntity.ok(modelMapper.map(session.get(), DeviceSessionDto.class));
            } else {
                return ResponseEntity.notFound().build();
            }
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
    }
    
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getHealth() {
        Map<String, Object> health = new HashMap<>();
        Collection<DeviceSession> sessions = deviceSessionService.getAllSessions();
        
        health.put("status", "UP");
        health.put("activeSessions", sessions.size());
        health.put("timestamp", java.time.Instant.now().toString());
        
        return ResponseEntity.ok(health);
    }
    
    @GetMapping("/")
    public ResponseEntity<Map<String, String>> getRoot() {
        Map<String, String> response = new HashMap<>();
        response.put("message", "Device Gateway Service is running!");
        response.put("version", "1.0.0");
        response.put("timestamp", java.time.Instant.now().toString());
        return ResponseEntity.ok(response);
    }
    
}

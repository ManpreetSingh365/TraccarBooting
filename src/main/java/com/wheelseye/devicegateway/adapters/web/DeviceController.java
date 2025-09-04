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
    

    private final ModelMapper modelMapper;
    
    public DeviceController(DeviceSessionService deviceSessionService, ModelMapper modelMapper) {
        this.modelMapper = modelMapper;
    }
    
    @GetMapping("/sessions")
    public ResponseEntity<String> getAllSessions() {
        
        return ResponseEntity.ok("OK");
    }
    
    @GetMapping("/{imei}/session")
    public ResponseEntity<String> getSessionByImei(@PathVariable String imei) {
        try {
            IMEI deviceImei = new IMEI(imei);
            
            return ResponseEntity.ok("OK");

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }
    }
    
    @GetMapping("/health")
    public ResponseEntity<String> getHealth() {

        
        return ResponseEntity.ok("health");
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

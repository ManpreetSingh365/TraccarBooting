package com.wheelseye.devicegateway.application.ports;

import com.wheelseye.devicegateway.domain.entities.DeviceSession;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;
import io.netty.channel.Channel;
import java.util.Optional;
import java.util.Collection;

public interface SessionRepository {
    void save(DeviceSession session);
    Optional<DeviceSession> findById(String sessionId);
    Optional<DeviceSession> findByImei(IMEI imei);
    Optional<DeviceSession> findByChannel(Channel channel);
    void deleteById(String sessionId);
    void deleteByChannel(Channel channel);
    Collection<DeviceSession> findAll();
    Collection<DeviceSession> findIdleSessions(long maxIdleSeconds);
}

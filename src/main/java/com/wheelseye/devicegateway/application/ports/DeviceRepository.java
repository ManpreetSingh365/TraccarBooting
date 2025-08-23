package com.wheelseye.devicegateway.application.ports;

import java.util.Optional;

import com.wheelseye.devicegateway.domain.entities.Device;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;

public interface DeviceRepository {
    void save(Device device);
    Optional<Device> findByImei(IMEI imei);
    boolean existsByImei(IMEI imei);
    void deleteByImei(IMEI imei);
}

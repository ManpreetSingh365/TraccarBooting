package com.wheelseye.devicegateway.domain.mappers;

import com.google.protobuf.Timestamp;
import com.wheelseye.devicegateway.domain.events.DeviceSessionEvent;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;

import java.time.Instant;

public class DeviceSessionEventMapper {

    // Domain -> Proto
    public static com.wheelseye.devicegateway.protobuf.DeviceSessionEvent toProto(DeviceSessionEvent event) {
        com.wheelseye.devicegateway.protobuf.DeviceSessionEvent.Builder builder =
                com.wheelseye.devicegateway.protobuf.DeviceSessionEvent.newBuilder()
                        .setEventType(event.getEventType())
                        .setSessionId(event.getSessionId())
                        .setImei(
                                com.wheelseye.devicegateway.protobuf.IMEI.newBuilder()
                                        .setValue(event.getImei().getValue())
                                        .build()
                        )
                        .setDeviceModel(event.getDeviceModel())
                        .setIpAddress(event.getIpAddress())
                        .setProtocolVersion(event.getProtocolVersion());

        // Convert Instant -> Timestamp
        Instant ts = event.getTimestamp();
        builder.setTimestamp(
                Timestamp.newBuilder()
                        .setSeconds(ts.getEpochSecond())
                        .setNanos(ts.getNano())
                        .build()
        );

        return builder.build();
    }

    // Proto -> Domain
    public static DeviceSessionEvent fromProto(com.wheelseye.devicegateway.protobuf.DeviceSessionEvent proto) {
        Instant timestamp = Instant.ofEpochSecond(
                proto.getTimestamp().getSeconds(),
                proto.getTimestamp().getNanos()
        );

        DeviceSessionEvent event = new DeviceSessionEvent(
                proto.getEventType(),
                proto.getSessionId(),
                new IMEI(proto.getImei().getValue()),
                proto.getDeviceModel(),
                proto.getIpAddress(),
                proto.getProtocolVersion()
        );

        // Timestamp is generated at construction time in your current class,
        // but if you want to preserve proto timestamp:
        // (Uncomment this line if you add a setter for timestamp in your class)
        // event.setTimestamp(timestamp);

        return event;
    }
}

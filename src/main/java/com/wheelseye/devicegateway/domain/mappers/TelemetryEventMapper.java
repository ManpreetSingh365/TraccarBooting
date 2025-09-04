package com.wheelseye.devicegateway.domain.mappers;

import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.wheelseye.devicegateway.domain.events.TelemetryEvent;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;
import com.wheelseye.devicegateway.domain.valueobjects.Location;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class TelemetryEventMapper {

    // Domain -> Proto
    public static com.wheelseye.devicegateway.protobuf.TelemetryEvent toProto(TelemetryEvent event) {
        com.wheelseye.devicegateway.protobuf.TelemetryEvent.Builder builder =
                com.wheelseye.devicegateway.protobuf.TelemetryEvent.newBuilder()
                        .setImei(
                                com.wheelseye.devicegateway.protobuf.IMEI.newBuilder()
                                        .setValue(event.getImei().getValue())
                                        .build()
                        )
                        .setMessageType(event.getMessageType())
                        .setRawFrame(event.getRawFrame() != null ? event.getRawFrame() : "")
                        .setBatteryLevel(event.getBatteryLevel() != null ? event.getBatteryLevel() : 0)
                        .setGpsSignalStrength(event.getGpsSignalStrength() != null ? event.getGpsSignalStrength() : "");

        // Location mapping
        if (event.getLocation() != null) {
            builder.setLocation(LocationMapper.toProto(event.getLocation()));
        }

        // Attributes mapping
        if (event.getAttributes() != null) {
            event.getAttributes().forEach((key, value) ->
                    builder.putAttributes(key,
                            Value.newBuilder().setStringValue(String.valueOf(value)).build())
            );
        }

        // Timestamp mapping
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
    public static TelemetryEvent fromProto(com.wheelseye.devicegateway.protobuf.TelemetryEvent proto) {
        IMEI imei = new IMEI(proto.getImei().getValue());

        Location location = proto.hasLocation() ? LocationMapper.fromProto(proto.getLocation()) : null;

        Map<String, Object> attributes = new HashMap<>();
        proto.getAttributesMap().forEach((key, value) -> attributes.put(key, value.getStringValue()));

        return new TelemetryEvent(
                imei,
                proto.getMessageType(),
                location,
                proto.getBatteryLevel(),
                proto.getGpsSignalStrength(),
                attributes,
                proto.getRawFrame()
        );
    }
}

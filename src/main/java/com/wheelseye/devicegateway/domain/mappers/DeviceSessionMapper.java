package com.wheelseye.devicegateway.domain.mappers;

import com.wheelseye.devicegateway.domain.entities.DeviceSession;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;

import java.time.Instant;

public class DeviceSessionMapper {

    // Entity -> Proto
    public static com.wheelseye.devicegateway.protobuf.DeviceSession toProto(DeviceSession entity) {
        com.wheelseye.devicegateway.protobuf.DeviceSession.Builder builder =
                com.wheelseye.devicegateway.protobuf.DeviceSession.newBuilder()
                        .setId(entity.getId())
                        .setImei(com.wheelseye.devicegateway.protobuf.IMEI.newBuilder().setValue(entity.getImei().getValue()).build())
                        .setChannelId(entity.getChannelId())
                        .setAuthenticated(entity.isAuthenticated())
                        .setRemoteAddress(entity.getRemoteAddress())
                        .setCreatedAt(
                                com.google.protobuf.Timestamp.newBuilder()
                                        .setSeconds(entity.getCreatedAt().getEpochSecond())
                                        .setNanos(entity.getCreatedAt().getNano())
                                        .build()
                        )
                        .setLastActivityAt(
                                com.google.protobuf.Timestamp.newBuilder()
                                        .setSeconds(entity.getLastActivityAt().getEpochSecond())
                                        .setNanos(entity.getLastActivityAt().getNano())
                                        .build()
                        );

        entity.getAttributes().forEach((key, value) ->
                builder.putAttributes(key, String.valueOf(value))
        );

        return builder.build();
    }

    public static DeviceSession fromProto(com.wheelseye.devicegateway.protobuf.DeviceSession proto) {



        DeviceSession entity = new DeviceSession(
                proto.getId(),
                new IMEI(proto.getImei().getValue())
        );

        entity.setImei(new com.wheelseye.devicegateway.domain.valueobjects.IMEI(proto.getImei().getValue()));
        entity.setChannelId(proto.getChannelId());
        entity.setAuthenticated(proto.getAuthenticated());
        entity.setRemoteAddress(proto.getRemoteAddress());
        entity.setCreatedAt(Instant.ofEpochSecond(proto.getCreatedAt().getSeconds(), proto.getCreatedAt().getNanos()));
        entity.setLastActivityAt(Instant.ofEpochSecond(proto.getLastActivityAt().getSeconds(), proto.getLastActivityAt().getNanos()));

        proto.getAttributesMap().forEach((key, value) ->
                entity.setAttribute(key, value)
        );

        return entity;
    }
}
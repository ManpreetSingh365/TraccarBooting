package com.wheelseye.devicegateway.domain.mappers;

import com.wheelseye.devicegateway.domain.valueobjects.Location;
import com.wheelseye.devicegateway.protobuf.Location.Builder;
import com.google.protobuf.util.Timestamps;

public class LocationMapper {

    // Map from value object to protobuf
    public static com.wheelseye.devicegateway.protobuf.Location toProto(Location location) {
        if (location == null) return null;

        Builder builder = com.wheelseye.devicegateway.protobuf.Location.newBuilder()
                .setLatitude(location.getLatitude())
                .setLongitude(location.getLongitude())
                .setAltitude(location.getAltitude())
                .setSpeed(location.getSpeed())
                .setCourse(location.getCourse())
                .setValid(location.isValid())
                .setSatellites(location.getSatellites());

        if (location.getTimestamp() != null) {
            builder.setTimestamp(Timestamps.fromMillis(location.getTimestamp().toEpochMilli()));
        }

        return builder.build();
    }

    // Map from protobuf to value object
    public static Location fromProto(com.wheelseye.devicegateway.protobuf.Location proto) {
        if (proto == null) return null;

        java.time.Instant timestamp = proto.hasTimestamp()
                ? java.time.Instant.ofEpochMilli(Timestamps.toMillis(proto.getTimestamp()))
                : null;

        return new Location(
                proto.getLatitude(),
                proto.getLongitude(),
                proto.getAltitude(),
                proto.getSpeed(),
                proto.getCourse(),
                proto.getValid(),
                timestamp,
                proto.getSatellites()
        );
    }
}

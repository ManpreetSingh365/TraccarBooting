package com.wheelseye.devicegateway.domain.mappers;

import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.wheelseye.devicegateway.domain.events.CommandEvent;
import com.wheelseye.devicegateway.domain.valueobjects.IMEI;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class CommandEventMapper {

    // Domain -> Proto
    public static com.wheelseye.devicegateway.protobuf.CommandEvent toProto(CommandEvent event) {
        com.wheelseye.devicegateway.protobuf.CommandEvent.Builder builder =
                com.wheelseye.devicegateway.protobuf.CommandEvent.newBuilder()
                        .setCommandId(event.getCommandId())
                        .setImei(
                                com.wheelseye.devicegateway.protobuf.IMEI.newBuilder()
                                        .setValue(event.getImei().getValue())
                                        .build()
                        )
                        .setCommand(event.getCommand())
                        .setPriority(event.getPriority() != null ? event.getPriority() : "")
                        .setRetryCount(event.getRetryCount())
                        .setMaxRetries(event.getMaxRetries());

        // Parameters mapping (Map<String, Object> -> map<string, Value>)
        if (event.getParameters() != null) {
            event.getParameters().forEach((key, value) ->
                    builder.putParameters(
                            key,
                            Value.newBuilder().setStringValue(String.valueOf(value)).build()
                    )
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
    public static CommandEvent fromProto(com.wheelseye.devicegateway.protobuf.CommandEvent proto) {
        IMEI imei = new IMEI(proto.getImei().getValue());

        // Parameters mapping (map<string, Value> -> Map<String, Object>)
        Map<String, Object> parameters = new HashMap<>();
        proto.getParametersMap().forEach((key, value) ->
                parameters.put(key, value.getStringValue())
        );

        return new CommandEvent(
                proto.getCommandId(),
                imei,
                proto.getCommand(),
                parameters,
                proto.getPriority(),
                proto.getRetryCount(),
                proto.getMaxRetries()
        );
    }
}

package com.wheelseye.devicegateway.domain.valueobjects;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Location - FULLY COMPATIBLE with your existing constructor
 * 
 * Enhanced with additional constructors for different timestamp formats
 */
public class Location {
    private final double latitude;
    private final double longitude;
    private final double altitude;
    private final double speed;
    private final int course;
    private final boolean valid;
    private final Instant timestamp;
    private final int satellites;

    // EXISTING CONSTRUCTOR - Keep exactly as is for compatibility
    public Location(double latitude, double longitude, double altitude, 
                   double speed, int course, boolean valid, Instant timestamp, int satellites) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
        this.speed = speed;
        this.course = course;
        this.valid = valid;
        this.timestamp = timestamp;
        this.satellites = satellites;
    }

    // NEW CONSTRUCTOR - For compatibility with fixed GT06ProtocolParser (String timestamp)
    public Location(double latitude, double longitude, double altitude, 
                   double speed, int course, int satellites, boolean valid, String timestampStr) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
        this.speed = speed;
        this.course = course;
        this.satellites = satellites;
        this.valid = valid;
        
        // Convert string timestamp to Instant
        Instant ts;
        try {
            LocalDateTime ldt = LocalDateTime.parse(timestampStr.replace(" ", "T"));
            ts = ldt.toInstant(ZoneOffset.UTC);
        } catch (Exception e) {
            ts = Instant.now();
        }
        this.timestamp = ts;
    }

    // NEW CONSTRUCTOR - Alternative with int parameters for course/satellites
    public Location(double latitude, double longitude, double altitude, 
                   int speed, int course, int satellites, boolean valid, String timestampStr) {
        this(latitude, longitude, altitude, (double) speed, course, satellites, valid, timestampStr);
    }

    // EXISTING GETTERS - Keep exactly as is
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }
    public double getAltitude() { return altitude; }
    public double getSpeed() { return speed; }
    public int getCourse() { return course; }
    public boolean isValid() { return valid; }
    public Instant getTimestamp() { return timestamp; }
    public int getSatellites() { return satellites; }

    // EXISTING toString - Keep exactly as is
    @Override
    public String toString() {
        return "Location{" +
                "latitude=" + latitude +
                ", longitude=" + longitude +
                ", altitude=" + altitude +
                ", speed=" + speed +
                ", course=" + course +
                ", valid=" + valid +
                ", timestamp=" + timestamp +
                ", satellites=" + satellites +
                '}';
    }
}
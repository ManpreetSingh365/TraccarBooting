package com.wheelseye.devicegateway.domain.valueobjects;

import java.time.Instant;

public class Location {
    private final double latitude;
    private final double longitude;
    private final double altitude;
    private final double speed;
    private final int course;
    private final boolean valid;
    private final Instant timestamp;
    private final int satellites;

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

    // Getters
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }
    public double getAltitude() { return altitude; }
    public double getSpeed() { return speed; }
    public int getCourse() { return course; }
    public boolean isValid() { return valid; }
    public Instant getTimestamp() { return timestamp; }
    public int getSatellites() { return satellites; }
}

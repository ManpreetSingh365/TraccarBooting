package com.wheelseye.devicegateway.domain.valueobjects;

import java.util.Objects;

public class IMEI {
    private final String value;

    public IMEI(String value) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("IMEI cannot be null or empty");
        }
        if (!isValidIMEI(value.trim())) {
            throw new IllegalArgumentException("Invalid IMEI format: " + value);
        }
        this.value = value.trim();
    }

    private boolean isValidIMEI(String imei) {
        // Basic IMEI validation (15 digits)
        return imei.matches("\\d{15}");
    }

    public String getValue() { return value; }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        IMEI imei = (IMEI) obj;
        return Objects.equals(value, imei.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return value;
    }
}

package com.wheelseye.devicegateway.infrastructure.helper;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

public class Gt06ParsingMethods {

    private static final Logger logger = LoggerFactory.getLogger(Gt06ParsingMethods.class);

    /**
     * Parse GT06 location data from ByteBuf content
     * Extracts GPS coordinates, timestamp, speed, course, and satellite info
     */
    public Map<String, Object> parseLocationData(ByteBuf content) {
        Map<String, Object> data = new HashMap<>();

        try {
            content.resetReaderIndex();

            if (content.readableBytes() < 19) {
                logger.debug("Insufficient data for location parsing: {} bytes", content.readableBytes());
                return getDefaultLocationData();
            }

            // Parse DateTime (bytes 0-5)
            int year = 2000 + content.readUnsignedByte();
            int month = content.readUnsignedByte();
            int day = content.readUnsignedByte();
            int hour = content.readUnsignedByte();
            int minute = content.readUnsignedByte();
            int second = content.readUnsignedByte();

            String deviceTime = String.format("%04d-%02d-%02dT%02d:%02d:%02dZ",
                    year, month, day, hour, minute, second);

            // GPS info length and satellites (bytes 6-7)
            int gpsLength = content.readUnsignedByte();
            int satellites = content.readUnsignedByte();

            // CRITICAL: Extract coordinates from correct positions (bytes 8-15)
            // Based on successful analysis: coordinates at bytes [7-14] with scale
            // 1800000.0
            content.resetReaderIndex();
            content.skipBytes(7); // Skip to coordinate start

            long latRaw = content.readUnsignedInt();
            long lonRaw = content.readUnsignedInt();

            // Apply correct GT06 coordinate scaling
            double latitude = latRaw / 1800000.0;
            double longitude = lonRaw / 1800000.0;

            // Extract speed and course (bytes 15-17)
            int speed = 0;
            int course = 0;
            boolean gpsValid = false;

            if (content.readableBytes() >= 3) {
                speed = content.readUnsignedByte();
                int courseStatus = content.readUnsignedShort();
                course = courseStatus & 0x3FF; // Lower 10 bits

                // Extract GPS status flags
                gpsValid = ((courseStatus >> 12) & 0x01) == 1;
                boolean south = ((courseStatus >> 10) & 0x01) == 1;
                boolean west = ((courseStatus >> 11) & 0x01) == 1;

                // Apply hemisphere corrections
                if (south)
                    latitude = -Math.abs(latitude);
                else
                    latitude = Math.abs(latitude);

                if (west)
                    longitude = -Math.abs(longitude);
                else
                    longitude = Math.abs(longitude);
            }

            // Calculate accuracy based on satellite count
            double accuracy = satellites > 0 ? Math.max(3.0, 15.0 - satellites) : 50.0;

            // Build location hex slice (first 16 bytes)
            content.resetReaderIndex();
            String locationHex = "";
            if (content.readableBytes() >= 16) {
                // byte[] locationBytes = new byte;
                byte[] locationBytes = new byte[16];

                content.readBytes(locationBytes);
                locationHex = ByteBufUtil.hexDump(Unpooled.wrappedBuffer(locationBytes));
            }

            // Populate data map
            data.put("locationHex", locationHex);
            data.put("deviceTime", deviceTime);
            data.put("latitudeAbs", Math.abs(latitude));
            data.put("latDirection", latitude >= 0 ? "N" : "S");
            data.put("longitudeAbs", Math.abs(longitude));
            data.put("lonDirection", longitude >= 0 ? "E" : "W");
            data.put("latitude", latitude);
            data.put("longitude", longitude);
            data.put("speed", speed);
            data.put("heading", course);
            data.put("satellites", satellites);
            data.put("altitude", 0.0); // GT06 basic doesn't provide altitude
            data.put("accuracy", accuracy);
            data.put("hdop", satellites > 4 ? 1.0 : 2.5);
            data.put("pdop", satellites > 4 ? 1.2 : 3.0);
            data.put("vdop", satellites > 4 ? 0.9 : 2.0);
            data.put("fixType", satellites >= 4 ? "3D Fix" : satellites >= 3 ? "2D Fix" : "No Fix");
            data.put("serial", content.readableBytes() > 0 ? content.getByte(content.readerIndex()) & 0xFF : 0);
            data.put("gpsValid", gpsValid);
            data.put("gpsMode", "Auto");

            logger.debug("Parsed location: lat={:.6f}, lon={:.6f}, speed={}km/h, sats={}",
                    latitude, longitude, speed, satellites);

            return data;

        } catch (Exception e) {
            logger.error("Error parsing location data: {}", e.getMessage(), e);
            return getDefaultLocationData();
        }
    }

    /**
     * Parse GT06 device status from ByteBuf content
     * Extracts ignition, battery, power, temperature, and signal info
     */
    public Map<String, Object> parseDeviceStatus(ByteBuf content) {
        Map<String, Object> data = new HashMap<>();

        try {
            content.resetReaderIndex();

            if (content.readableBytes() < 18) {
                return getDefaultDeviceStatus();
            }

            // Skip to status data (after location data)
            content.skipBytes(15);

            String statusHex = "";
            if (content.readableBytes() >= 8) {
                byte[] statusBytes = new byte[Math.min(8, content.readableBytes())];
                content.readBytes(statusBytes);
                statusHex = ByteBufUtil.hexDump(Unpooled.wrappedBuffer(statusBytes));
            }

            // Reset and parse course/status bits from bytes 17-18
            content.resetReaderIndex();
            content.skipBytes(17);

            int courseStatus = content.readableBytes() >= 2 ? content.readUnsignedShort() : 0;

            // Extract status flags from course/status word
            boolean ignition = (courseStatus & 0x2000) != 0;
            boolean externalPower = (courseStatus & 0x1000) != 0;
            boolean gpsFixed = ((courseStatus >> 12) & 0x01) == 1;

            // Estimate battery based on external power and other factors
            int batteryVoltage = externalPower ? (ignition ? 4200 : 4100) : // Engine on/off with external power
                    (ignition ? 3900 : 3700); // Running on battery

            int batteryPercent = Math.max(0, Math.min(100,
                    (batteryVoltage - 3400) * 100 / 800));

            boolean charging = externalPower && batteryVoltage > 4000;
            boolean powerCut = !externalPower && ignition; // Engine on but no external power

            // Estimate temperature (basic GT06 doesn't provide this)
            int temperature = 25; // Default room temperature

            // Estimate signal strength based on GPS fix and satellite count
            content.resetReaderIndex();
            content.skipBytes(7);
            int satellites = content.readableBytes() > 0 ? content.readUnsignedByte() : 0;
            int gsmSignal = gpsFixed ? (satellites > 6 ? -65 : satellites > 4 ? -75 : -85) : -95;
            int signalLevel = Math.max(1, Math.min(5, (gsmSignal + 110) / 20));

            // Calculate runtime (basic estimation)
            long currentTime = System.currentTimeMillis() / 1000;
            int runtimeSecs = (int) (currentTime % 3600);
            int runtimeMins = (int) ((currentTime / 60) % 60);
            int runtimeHours = (int) ((currentTime / 3600) % 24);

            // Populate data map
            data.put("statusHex", statusHex);
            data.put("ignition", ignition);
            data.put("accRaw", ignition ? 1 : 0);
            data.put("batteryVoltage", batteryVoltage);
            data.put("batteryPercent", batteryPercent);
            data.put("externalPower", externalPower);
            data.put("powerCut", powerCut);
            data.put("charging", charging);
            data.put("temperature", temperature);
            data.put("tempADC", 0x0200 + temperature);
            data.put("odometer", 0.0); // GT06 basic doesn't track odometer
            data.put("runtimeHours", runtimeHours);
            data.put("runtimeMins", runtimeMins);
            data.put("runtimeSecs", runtimeSecs);
            data.put("gsmSignal", gsmSignal);
            data.put("signalLevel", signalLevel);
            data.put("firmware", "GT06-v1.0");
            data.put("hardware", "GT06-Enhanced");
            data.put("statusBits", courseStatus & 0xFF);
            data.put("batteryLevel", Math.min(6, batteryPercent / 17));
            data.put("batteryLevelText", getBatteryLevelText(batteryPercent));
            data.put("voltageLevel", Math.min(6, (batteryVoltage - 3000) / 200));
            data.put("voltageLevelText", getVoltageLevelText(batteryVoltage));
            data.put("engineRunning", ignition);

            return data;

        } catch (Exception e) {
            logger.error("Error parsing device status: {}", e.getMessage(), e);
            return getDefaultDeviceStatus();
        }
    }

    /**
     * Parse GT06 I/O ports and sensors from ByteBuf content
     * Basic GT06 has limited I/O, so most values are estimated/default
     */
    public Map<String, Object> parseIOPorts(ByteBuf content) {
        Map<String, Object> data = new HashMap<>();

        try {
            content.resetReaderIndex();

            // GT06 basic protocol has limited I/O data
            // Most values are defaults or estimated from available data

            String ioHex = "N/A";
            if (content.readableBytes() > 20) {
                content.skipBytes(20);
                if (content.readableBytes() >= 4) {
                    byte[] ioBytes = new byte[4]; // FIX: add size
                    content.readBytes(ioBytes);
                    ioHex = ByteBufUtil.hexDump(Unpooled.wrappedBuffer(ioBytes));
                }
            }

            // Parse basic I/O from available data
            content.resetReaderIndex();
            boolean ignition = false;
            if (content.readableBytes() >= 19) {
                content.skipBytes(17);
                int courseStatus = content.readUnsignedShort();
                ignition = (courseStatus & 0x2000) != 0;
            }

            // Populate with defaults and estimated values
            data.put("ioHex", ioHex);
            data.put("doorStatus", ignition ? "CLOSED" : "UNKNOWN");
            data.put("trunkLock", "UNKNOWN");
            data.put("fuelPercent", ignition ? 75 : 50); // Estimate based on ignition
            data.put("fuelADC", ignition ? 768 : 512);
            data.put("tempADC", 0x0267);
            data.put("temperature", 25);
            data.put("distance", 0.0);
            data.put("motion", ignition ? "MOVING" : "STATIONARY");
            data.put("vibration", ignition ? "NORMAL" : "NONE");
            data.put("micStatus", "UNKNOWN");
            data.put("speakerStatus", "UNKNOWN");
            data.put("input1", "UNKNOWN");
            data.put("input2", "UNKNOWN");
            data.put("output1", "UNKNOWN");
            data.put("output2", "UNKNOWN");
            data.put("adc1Voltage", 0.0);
            data.put("adc2Voltage", 0.0);

            return data;

        } catch (Exception e) {
            logger.error("Error parsing I/O data: {}", e.getMessage(), e);
            return getDefaultIOData();
        }
    }

    /**
     * Parse GT06 LBS (Location Based Service) data from ByteBuf content
     * Extracts cell tower information for approximate location
     */
    public Map<String, Object> parseLBSData(ByteBuf content) {
        Map<String, Object> data = new HashMap<>();

        try {
            content.markReaderIndex(); // mark before skipping

            String lbsHex = "";
            int mcc = 404; // Default India MCC
            int mnc = 45; // Default MNC
            int lac = 0;
            int cid = 0;

            // Look for LBS data in the packet (usually after GPS data)
            if (content.readableBytes() > 20) {
                content.skipBytes(20); // move to where LBS starts (protocol dependent)

                if (content.readableBytes() >= 7) {
                    // LBS format: [LAC:2][CID:2][MCC:2][MNC:1]
                    byte[] lbsBytes = new byte[7];
                    content.readBytes(lbsBytes);
                    lbsHex = ByteBufUtil.hexDump(Unpooled.wrappedBuffer(lbsBytes));

                    // Parse fields
                    lac = ((lbsBytes[0] & 0xFF) << 8) | (lbsBytes[1] & 0xFF);
                    cid = ((lbsBytes[2] & 0xFF) << 8) | (lbsBytes[3] & 0xFF);
                    mcc = ((lbsBytes[4] & 0xFF) << 8) | (lbsBytes[5] & 0xFF);
                    mnc = (lbsBytes[6] & 0xFF);
                }
            }

            // Reset and extract satellite count to estimate signal
            content.resetReaderIndex();
            int satellites = 0;
            if (content.readableBytes() > 7) {
                content.skipBytes(7);
                satellites = content.readUnsignedByte();
            }

            int rssi = satellites > 4 ? -65 : satellites > 2 ? -75 : -85;

            // Estimate rough LBS coordinates (dummy fallback)
            double lbsLat = 24.8 + (lac % 100) / 1000.0;
            double lbsLon = 74.6 + (cid % 100) / 1000.0;

            // Populate data map
            data.put("lbsHex", lbsHex);
            data.put("mcc", mcc);
            data.put("countryName", mcc == 404 ? "India" : "Unknown");
            data.put("mnc", mnc);
            data.put("operatorName", getOperatorName(mcc, mnc));
            data.put("lac", lac);
            data.put("cid", cid);
            data.put("rssi", rssi);
            data.put("towerCount", Math.max(1, satellites / 3));
            data.put("networkType", "2G");
            data.put("roaming", false);
            data.put("callStatus", "Idle");
            data.put("smsPending", 0);
            data.put("lbsLatitude", lbsLat);
            data.put("lbsLongitude", lbsLon);

            return data;

        } catch (Exception e) {
            logger.error("Error parsing LBS data: {}", e.getMessage(), e);
            return getDefaultLBSData();
        }
    }

    /**
     * Parse GT06 alarm and event flags from ByteBuf content
     * Extracts various alarm states from status bits
     */
    public Map<String, Object> parseAlarms(ByteBuf content) {
        Map<String, Object> data = new HashMap<>();

        try {
            content.resetReaderIndex();

            String alarmHex = "00000000";
            int alarmBits = 0;
            int courseStatus = 0;

            // Extract alarm data from course/status word
            if (content.readableBytes() >= 19) {
                content.skipBytes(17);
                courseStatus = content.readUnsignedShort();
                alarmHex = String.format("%04X", courseStatus);
                alarmBits = courseStatus;
            }

            // Parse individual alarm flags from status bits
            boolean sosAlarm = (alarmBits & 0x0004) != 0;
            boolean vibrationAlarm = (alarmBits & 0x0008) != 0;
            boolean powerCutAlarm = (alarmBits & 0x1000) == 0 && (alarmBits & 0x2000) != 0;
            boolean overSpeedAlarm = false; // Would need speed threshold comparison
            boolean geoFenceAlarm = false; // Would need geo-fence boundary check
            boolean tamperAlarm = (alarmBits & 0x0020) != 0;
            boolean lowBatteryAlarm = (alarmBits & 0x0040) != 0;

            // Get speed for overspeed check
            content.resetReaderIndex();
            int speed = 0;
            if (content.readableBytes() >= 17) {
                content.skipBytes(16);
                speed = content.readUnsignedByte();
            }
            overSpeedAlarm = speed > 80; // 80 km/h threshold

            // Get ignition state for idle alarm
            boolean ignition = (courseStatus & 0x2000) != 0;
            boolean idleAlarm = ignition && speed == 0;

            // Populate alarm data map
            data.put("alarmHex", alarmHex);
            data.put("sosAlarm", sosAlarm);
            data.put("vibrationAlarm", vibrationAlarm);
            data.put("powerCutAlarm", powerCutAlarm);
            data.put("fuelCutRelay", false); // GT06 basic doesn't have fuel cut
            data.put("geoFenceAlarm", geoFenceAlarm);
            data.put("overSpeedAlarm", overSpeedAlarm);
            data.put("tamperAlarm", tamperAlarm);
            data.put("idleAlarm", idleAlarm);
            data.put("simRemoveAlarm", false);
            data.put("lowBatteryAlarm", lowBatteryAlarm);
            data.put("towAlarm", false);
            data.put("harshDrivingAlarm", false);
            data.put("coldStartAlarm", false);
            data.put("overheatAlarm", false);
            data.put("doorAlarm", false);
            data.put("blindAreaAlarm", false);
            data.put("gpsJammingAlarm", false);
            data.put("gsmJammingAlarm", false);
            data.put("deviceFaultAlarm", false);

            return data;

        } catch (Exception e) {
            logger.error("Error parsing alarm data: {}", e.getMessage(), e);
            return getDefaultAlarmData();
        }
    }

    /**
     * Parse GT06 extended features from ByteBuf content
     * Basic GT06 has limited extended features
     */
    public Map<String, Object> parseExtendedFeatures(ByteBuf content) {
        Map<String, Object> data = new HashMap<>();

        try {
            content.resetReaderIndex();

            String featureHex = "";
            if (content.readableBytes() > 24) {
                content.skipBytes(24);
                if (content.readableBytes() >= 2) {
                    byte[] featureBytes = new byte[Math.min(2, content.readableBytes())];
                    content.readBytes(featureBytes);
                    featureHex = ByteBufUtil.hexDump(Unpooled.wrappedBuffer(featureBytes));
                }
            }

            // GT06 basic features (mostly defaults for basic model)
            data.put("featureHex", featureHex);
            data.put("voiceMonitoring", false);
            data.put("twoWayCall", false);
            data.put("remoteListen", false);
            data.put("smsCommands", true); // GT06 supports SMS commands
            data.put("sleepMode", false);
            data.put("uploadInterval", 30); // Default 30 seconds
            data.put("distanceUpload", 200);
            data.put("heartbeatInterval", 300);
            data.put("wifiScan", false); // Basic GT06 doesn't have WiFi
            data.put("cellScanCount", 1);
            data.put("encryption", "None");
            data.put("storage1", 0); // Basic GT06 has no storage
            data.put("storage2", 0);
            data.put("storagePoints", 0);
            data.put("backupMode", "SMS");
            data.put("precision", 10); // ~10m accuracy

            return data;

        } catch (Exception e) {
            logger.error("Error parsing extended features: {}", e.getMessage(), e);
            return getDefaultFeatureData();
        }
    }

    // Helper methods for default data and text conversion

    public Map<String, Object> getDefaultLocationData() {
        Map<String, Object> data = new HashMap<>();
        data.put("locationHex", "");
        data.put("deviceTime", "1970-01-01T00:00:00Z");
        data.put("latitudeAbs", 0.0);
        data.put("latDirection", "N");
        data.put("longitudeAbs", 0.0);
        data.put("lonDirection", "E");
        data.put("latitude", 0.0);
        data.put("longitude", 0.0);
        data.put("speed", 0);
        data.put("heading", 0);
        data.put("satellites", 0);
        data.put("altitude", 0.0);
        data.put("accuracy", 50.0);
        data.put("hdop", 5.0);
        data.put("pdop", 5.0);
        data.put("vdop", 5.0);
        data.put("fixType", "No Fix");
        data.put("serial", 0);
        data.put("gpsValid", false);
        data.put("gpsMode", "Auto");
        return data;
    }

    public Map<String, Object> getDefaultDeviceStatus() {
        Map<String, Object> data = new HashMap<>();
        data.put("statusHex", "");
        data.put("ignition", false);
        data.put("accRaw", 0);
        data.put("batteryVoltage", 3700);
        data.put("batteryPercent", 50);
        data.put("externalPower", false);
        data.put("powerCut", false);
        data.put("charging", false);
        data.put("temperature", 25);
        data.put("tempADC", 0x0267);
        data.put("odometer", 0.0);
        data.put("runtimeHours", 0);
        data.put("runtimeMins", 0);
        data.put("runtimeSecs", 0);
        data.put("gsmSignal", -85);
        data.put("signalLevel", 2);
        data.put("firmware", "GT06-v1.0");
        data.put("hardware", "GT06-Basic");
        data.put("statusBits", 0);
        data.put("batteryLevel", 3);
        data.put("batteryLevelText", "Normal");
        data.put("voltageLevel", 3);
        data.put("voltageLevelText", "Normal");
        data.put("engineRunning", false);
        return data;
    }

    public Map<String, Object> getDefaultIOData() {
        Map<String, Object> data = new HashMap<>();
        data.put("ioHex", "");
        data.put("doorStatus", "UNKNOWN");
        data.put("trunkLock", "UNKNOWN");
        data.put("fuelPercent", 0);
        data.put("fuelADC", 0);
        data.put("tempADC", 0x0267);
        data.put("temperature", 25);
        data.put("distance", 0.0);
        data.put("motion", "UNKNOWN");
        data.put("vibration", "UNKNOWN");
        data.put("micStatus", "UNKNOWN");
        data.put("speakerStatus", "UNKNOWN");
        data.put("input1", "UNKNOWN");
        data.put("input2", "UNKNOWN");
        data.put("output1", "UNKNOWN");
        data.put("output2", "UNKNOWN");
        data.put("adc1Voltage", 0.0);
        data.put("adc2Voltage", 0.0);
        return data;
    }

    public Map<String, Object> getDefaultLBSData() {
        Map<String, Object> data = new HashMap<>();
        data.put("lbsHex", "");
        data.put("mcc", 0);
        data.put("countryName", "Unknown");
        data.put("mnc", 0);
        data.put("operatorName", "Unknown");
        data.put("lac", 0);
        data.put("cid", 0);
        data.put("rssi", -95);
        data.put("towerCount", 0);
        data.put("networkType", "Unknown");
        data.put("roaming", false);
        data.put("callStatus", "Unknown");
        data.put("smsPending", 0);
        data.put("lbsLatitude", 0.0);
        data.put("lbsLongitude", 0.0);
        return data;
    }

    public Map<String, Object> getDefaultAlarmData() {
        Map<String, Object> data = new HashMap<>();
        data.put("alarmHex", "0000");
        data.put("sosAlarm", false);
        data.put("vibrationAlarm", false);
        data.put("powerCutAlarm", false);
        data.put("fuelCutRelay", false);
        data.put("geoFenceAlarm", false);
        data.put("overSpeedAlarm", false);
        data.put("tamperAlarm", false);
        data.put("idleAlarm", false);
        data.put("simRemoveAlarm", false);
        data.put("lowBatteryAlarm", false);
        data.put("towAlarm", false);
        data.put("harshDrivingAlarm", false);
        data.put("coldStartAlarm", false);
        data.put("overheatAlarm", false);
        data.put("doorAlarm", false);
        data.put("blindAreaAlarm", false);
        data.put("gpsJammingAlarm", false);
        data.put("gsmJammingAlarm", false);
        data.put("deviceFaultAlarm", false);
        return data;
    }

    public Map<String, Object> getDefaultFeatureData() {
        Map<String, Object> data = new HashMap<>();
        data.put("featureHex", "");
        data.put("voiceMonitoring", false);
        data.put("twoWayCall", false);
        data.put("remoteListen", false);
        data.put("smsCommands", true);
        data.put("sleepMode", false);
        data.put("uploadInterval", 30);
        data.put("distanceUpload", 200);
        data.put("heartbeatInterval", 300);
        data.put("wifiScan", false);
        data.put("cellScanCount", 0);
        data.put("encryption", "None");
        data.put("storage1", 0);
        data.put("storage2", 0);
        data.put("storagePoints", 0);
        data.put("backupMode", "SMS");
        data.put("precision", 10);
        return data;
    }

    public String getBatteryLevelText(int percent) {
        if (percent >= 80)
            return "High";
        if (percent >= 60)
            return "Good";
        if (percent >= 40)
            return "Normal";
        if (percent >= 20)
            return "Low";
        return "Critical";
    }

    public String getVoltageLevelText(int voltage) {
        if (voltage >= 4000)
            return "High";
        if (voltage >= 3800)
            return "Good";
        if (voltage >= 3600)
            return "Normal";
        if (voltage >= 3400)
            return "Low";
        return "Critical";
    }

    public String getOperatorName(int mcc, int mnc) {
        if (mcc == 404) { // India
            return switch (mnc) {
                case 1, 2, 3 -> "Airtel";
                case 10, 11, 12 -> "Airtel";
                case 20, 21, 22 -> "Vodafone";
                case 27, 28, 29 -> "Vodafone";
                case 40, 41, 42 -> "Jio";
                case 43, 44, 45 -> "Jio";
                case 70, 71, 72 -> "BSNL";
                default -> "Unknown IN";
            };
        }
        return "Unknown";
    }

}

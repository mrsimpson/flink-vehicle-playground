package io.github.mrsimpson.vehicleStreaming.util;

public class Location {
    public double latitude;
    public double longitude;

    // empty default constructor required for Jackson De-Serialization
    public Location(){}

    public Location(double latitude, double longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public String toFormattedString() {
        return "(" + latitude + "," + longitude + ")";
    }

}


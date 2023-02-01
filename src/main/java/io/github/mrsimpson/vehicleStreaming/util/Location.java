package io.github.mrsimpson.vehicleStreaming.util;

public class Location {
    double latitude;
    double longitude;

    public Location(double latitude, double longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public String toString() {
        return "(" + latitude + "," + longitude + ")";
    }

}


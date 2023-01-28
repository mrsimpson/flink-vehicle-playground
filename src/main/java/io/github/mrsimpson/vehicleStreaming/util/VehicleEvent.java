package io.github.mrsimpson.vehicleStreaming.util;

import java.util.Calendar;

public class VehicleEvent {
    public String id;
    public long ts;
    public double latitude;
    public double longitude;

    public VehicleEvent(String id, double latitude, double longitude){
        this.id = id;
        this.latitude = latitude;
        this.longitude = longitude;
        this.ts = Calendar.getInstance().getTimeInMillis();
    }

    @Override
    public String toString() {
        return id + ": (" + latitude + "," + longitude + ")";
    }

}

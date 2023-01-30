package io.github.mrsimpson.vehicleStreaming.util;

import java.util.Calendar;

public class VehicleEvent {
    public String id;
    public long ts;
    public double latitude;
    public double longitude;
    public VehicleEventType type;
    public VehicleStateType newState;

    public VehicleEvent(String id, double latitude, double longitude, VehicleEventType type, VehicleStateType newState) {
        this.id = id;
        this.latitude = latitude;
        this.longitude = longitude;
        this.type = type;
        this.newState = newState;
        this.ts = Calendar.getInstance().getTimeInMillis();
    }

    @Override
    public String toString() {
        return id + " emits " + type + ", is now " + newState + " at (" + latitude + "," + longitude + ")";
    }

}

package io.github.mrsimpson.vehicleStreaming.util;

import java.util.Calendar;

public class VehicleEvent {
    public String id;
    public long ts;
    public String provider;
    public Location location;
    public VehicleEventType type;
    public VehicleStateType newState;

    public VehicleEvent(String id,
                        String provider,
                        Location location,
                        VehicleEventType type,
                        VehicleStateType newState) {
        this.id = id;
        this.provider = provider;
        this.location = location;
        this.type = type;
        this.newState = newState;
        this.ts = Calendar.getInstance().getTimeInMillis();
    }

    @Override
    public String toString() {
        return id + " of " + provider + " emits " + type + ", is now " + newState + " at (" + location.latitude + "," + location.longitude + ")";
    }

}

package io.github.mrsimpson.vehicleStreaming.util;

import java.util.Date;

public class VehicleEvent {
    public String id;
    public Date eventDate;
    public String provider;
    public Location location;
    public VehicleEventType type;
    public VehicleStateType newState;

    public VehicleEvent(String id,
                        Date eventDate,
                        String provider,
                        Location location,
                        VehicleEventType type,
                        VehicleStateType newState) {
        this.id = id;
        this.eventDate = eventDate;
        this.provider = provider;
        this.location = location;
        this.type = type;
        this.newState = newState;
    }

    public VehicleEvent(String id,
                        String provider,
                        Location location,
                        VehicleEventType type,
                        VehicleStateType newState) {
        this(id, new Date(), provider, location, type, newState);
    }

    @Override
    public String toString() {
        return id + " of " + provider + " emits " + type + ", is now " + newState + " at " + location;
    }

}

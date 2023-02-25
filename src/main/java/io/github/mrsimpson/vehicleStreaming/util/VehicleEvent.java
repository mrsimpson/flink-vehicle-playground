package io.github.mrsimpson.vehicleStreaming.util;

import java.util.Date;

public class VehicleEvent implements FormattablePojo {

    public String id;
    public Date eventTime;
    public String provider;
    public Location location;
    public VehicleEventType type;
    public VehicleStateType newState;

    // empty default constructor required for Jackson De-Serialization
    public VehicleEvent(){}

    public VehicleEvent(String id,
                        Date eventTime,
                        String provider,
                        Location location,
                        VehicleEventType type,
                        VehicleStateType newState) {
        this.id = id;
        this.eventTime = eventTime;
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

    public String toFormattedString() {
        return "At " + eventTime + ", " + id + " of " + provider + " emits " + type + ", is now " + newState + " at " + location.toFormattedString();
    }
}

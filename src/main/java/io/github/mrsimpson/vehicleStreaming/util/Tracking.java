package io.github.mrsimpson.vehicleStreaming.util;

import java.util.Date;

public class Tracking {
    public Date date;
    public Location location;

    public Tracking(Date date, Location location) {
        this.date = date;
        this.location = location;
    }

    @Override
    public String toString() {
        return "at " + date + " " + location;
    }
}

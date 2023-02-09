package io.github.mrsimpson.vehicleStreaming.util;

import java.util.Date;

public class Tracking implements FormattablePojo{
    public Date date;
    public Location location;

    public Tracking(Date date, Location location) {
        this.date = date;
        this.location = location;
    }

    public String toFormattedString() {
        return "at " + date + " " + location.toFormattedString();
    }
}

package io.github.mrsimpson.vehicleStreaming.util;

import java.time.Duration;
import java.util.Date;

public class ParkingInterval implements FormattablePojo {

    public String id;
    public String provider;
    public Location location;
    public Date start;
    public Date end;
    public Duration resolution;
    public Date scheduledEnd;

    // empty default constructor required for Jackson De-Serialization
    public ParkingInterval() {
    }

    public ParkingInterval(String id, String provider, Location location, Date start) {
        this.id = id;
        this.provider = provider;
        this.location = location;
        this.start = start;
    }

    public void terminate(Date end) {
        this.end = end;
    }

    public void scheduleEnd(Date end) {
        this.scheduledEnd = end;
    }

    public long getParkingDurationMillis() {
        // we'll consider only completed intervals for simplicity in this example.
        return this.end != null ? this.end.getTime() - this.start.getTime() : 0;
    }

    /**
     * Calculates how long a parking interval was related to the resolution with which it was recorded
     *
     * @return ratio between parking duration and resolution
     */
    public double getAvailability() {
        return (double) this.getParkingDurationMillis() / this.resolution.toMillis();
    }

    @Override
    public String toFormattedString() {
        return id + " of " + provider
                + (end == null ? " is parking since " + start : " was parked from " + start + " until " + end + ", availability = " + getAvailability())
                + " at " + location.toFormattedString();
    }
}

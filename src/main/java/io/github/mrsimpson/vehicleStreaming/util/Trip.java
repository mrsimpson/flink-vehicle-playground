package io.github.mrsimpson.vehicleStreaming.util;

import java.util.ArrayList;

public class Trip implements FormattablePojo {

    public String vehicleId;
    public Tracking start;
    public Tracking end;

    private Boolean ongoing = true;

    public ArrayList<Tracking> waypoints = new ArrayList<>();

    public Trip(String vehicleId, Tracking start) {
        this.vehicleId = vehicleId;
        this.start = start;
    }

    public Trip(String vehicleId, Tracking start, Tracking end) {
        this.vehicleId = vehicleId;
        this.start = start;
        this.end = end;
    }

    public String toFormattedString(){
        return "Trip of " + vehicleId + " started " + start +
                (ongoing ? " ongoing" : " --> " + end);
    }

    public void terminate() {
        this.ongoing = false;
    }

    public void terminate(Tracking end) {
        this.end = end;
        this.terminate();
    }

    public void addWaypoint(Tracking tracking) {
        this.waypoints.add(tracking);
    }
}

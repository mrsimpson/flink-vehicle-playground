package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Assigns timestamps to VehicleEvents based on their internal timestamp and
 * emits watermarks with a minute real time slack.
 */
public class VehicleEventsTimerAssigner extends BoundedOutOfOrdernessTimestampExtractor<VehicleEvent> {

    /**
     * Allow one minute of real time slack
     */
    public VehicleEventsTimerAssigner() {
        super(Time.minutes(1));
    }

    @Override
    public long extractTimestamp(VehicleEvent vehicleEvent) {
        return vehicleEvent.eventTime.getTime();
    }
}

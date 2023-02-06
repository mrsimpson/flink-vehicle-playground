package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

public class VehicleEventsTimestampAssigner implements SerializableTimestampAssigner<VehicleEvent> {

    @Override
    public long extractTimestamp(VehicleEvent element, long recordTimestamp) {
        return element.eventTime.getTime();
    }
}

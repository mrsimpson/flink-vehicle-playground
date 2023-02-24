package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import org.apache.flink.api.common.eventtime.*;

import java.time.Duration;

public interface VehicleEventsWatermarkStrategy {
    static WatermarkStrategy<VehicleEvent> withOutOfOrderSeconds(Integer latency) {
        return WatermarkStrategy
                .<VehicleEvent>forBoundedOutOfOrderness(Duration.ofSeconds(latency))
                .withTimestampAssigner((vehicleEvent, previousTimestamp) -> vehicleEvent.eventTime.getTime());
    }
}

package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import org.apache.flink.api.common.eventtime.*;

public abstract class VehicleEventsWatermarkStrategy implements WatermarkStrategy<VehicleEvent> {
    @Override
    public TimestampAssigner<VehicleEvent> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new VehicleEventsTimestampAssigner();
    }
}

package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.Tracking;
import io.github.mrsimpson.vehicleStreaming.util.Trip;
import io.github.mrsimpson.vehicleStreaming.util.TripTuple;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class TripConstructorFunction extends KeyedProcessFunction<String, VehicleEvent, TripTuple> {
    public static final OutputTag<Error> ERROR_OUTPUT_TAG = new OutputTag<>("trips-errors") {
    };

    private transient ValueState<Trip> currentTrip;

    private Tracking trackingFromEvent(VehicleEvent vehicleEvent) {
        return new Tracking(vehicleEvent.eventTime, vehicleEvent.location);
    }

    @Override
    public void processElement(VehicleEvent vehicleEvent, Context ctx, Collector<TripTuple> out) throws Exception {

        // as state, we need to remember the vehicle's current trip
        Trip trip = currentTrip.value();

        switch (vehicleEvent.type) {
            case TRIP_START:
                if (trip != null) {
                    ctx.output(ERROR_OUTPUT_TAG, new Error("Trip start detected though a trip is still ongoing"));
                    return;
                }

                // construct a new trip and buffer it
                trip = new Trip(vehicleEvent.id, trackingFromEvent(vehicleEvent));
                currentTrip.update(trip);
                break;
            case TRIP_END:
                if (trip == null) {
                    ctx.output(ERROR_OUTPUT_TAG, new Error("Trip end detected though NO trip is ongoing"));
                    return;
                }

                // terminate the trip and reset the buffer
                trip.terminate(trackingFromEvent(vehicleEvent));
                currentTrip.update(null);

                break;
            case LOCATED:
                if (trip != null) {
                    trip.addWaypoint(trackingFromEvent(vehicleEvent));
                }
                break;
        }

        // publish it
        if(trip != null){
            out.collect(new TripTuple(ctx.getCurrentKey(), trip));
        }
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Trip> valueStateDescriptor = new ValueStateDescriptor<>("currentTrip", Trip.class);
        currentTrip = getRuntimeContext().getState(valueStateDescriptor);
    }
}

package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.Tracking;
import io.github.mrsimpson.vehicleStreaming.util.Trip;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TripConstructorFunction extends KeyedProcessFunction<String, VehicleEvent, Tuple2<String, Trip>> {
    private transient ValueState<Trip> currentTrip;

    private Tracking trackingFromEvent(VehicleEvent vehicleEvent) {
        return new Tracking(vehicleEvent.eventDate, vehicleEvent.location);
    }

    @Override
    public void processElement(VehicleEvent vehicleEvent, Context ctx, Collector<Tuple2<String, Trip>> out) throws Exception {
        Trip trip = currentTrip.value();
        switch (vehicleEvent.type) {
            case TRIP_START:
                if (trip != null) {
                    throw new Exception("Trip start detected though a trip is still ongoing");
                }

                // construct a new trip and buffer it
                trip = new Trip(vehicleEvent.id, trackingFromEvent(vehicleEvent));
                currentTrip.update(trip);

                // publish it
                out.collect(new Tuple2<>(ctx.getCurrentKey(), trip));
                break;
            case TRIP_END:
                if (trip == null) {
                    throw new Exception("Trip end detected though NO trip is ongoing");
                }

                // terminate the trip and reset the buffer
                trip.terminate(trackingFromEvent(vehicleEvent));
                currentTrip.update(null);

                // publish it
                out.collect(new Tuple2<>(ctx.getCurrentKey(), trip));
                break;
            case LOCATED:
                trip.addWaypoint(trackingFromEvent(vehicleEvent));
                break;
        }
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Trip> valueStateDescriptor = new ValueStateDescriptor<>("currentTrip", Trip.class);
        currentTrip = getRuntimeContext().getState(valueStateDescriptor);
    }
}

package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.ParkingInterval;
import io.github.mrsimpson.vehicleStreaming.util.ParkingIntervalTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class WindowedAvailabilityAggregateFunction implements AggregateFunction<ParkingIntervalTuple, Tuple2<String, Long>, Tuple2<String, Long>> {
    /**
     * The accumulator itself is stateless, you can consider this method equivalent to
     * the last parameter of a Javascript reduce() function in which the "state", which is passed from iteration
     * to iteration is being initialized.
     *
     * @return The initial value. It holds everything which is needed to emit the result at the end of the window
     * In this case, this is the provider, the total duration of available vehicles and start and end of the window.
     * We'll get all of this from the parking intervals themselves, as we don't have access to the window metadata
     * If we used a generic ProcessWindowFunction, we could get this from the context.
     */
    @Override
    public Tuple2<String, Long> createAccumulator() {
        return new Tuple2<>("", 0L);
    }

    /**
     * The actual aggregation happens here
     * @param parkingIntervalTuple: the interval detected for a vehicle
     * @param acc: the previous availability for vehicles of this provider within the window
     * @return the updated availability for vehicles of this provider within the window
     */
    @Override
    public Tuple2<String, Long> add(ParkingIntervalTuple parkingIntervalTuple, Tuple2<String, Long> acc) {
        ParkingInterval parkingInterval = parkingIntervalTuple.f1;
        return new Tuple2<>(
                parkingInterval.provider,
                acc.f1 + parkingInterval.getParkingDurationMillis()
        );
    }

    /**
     * This method is being called at the end of the window. Its result will be emitted downstream
     * @param acc: The accumulation aggregated during the window
     * @return the total available ratio based on the time window's size
     */
    @Override
    public Tuple2<String, Long> getResult(Tuple2<String, Long> acc) {

        return new Tuple2<>(acc.f0, acc.f1);
    }

    /**
     * This is being called if two intermediate states (no matter how they come into existence though) get aggregated
     * @param acc1: The first intermediate state
     * @param acc2: The second intermediate state
     * @return The merged state
     */
    @Override
    public Tuple2<String, Long> merge(Tuple2<String, Long> acc1, Tuple2<String, Long> acc2) {
        return new Tuple2<>(
                acc1.f0,
                acc1.f1 + acc2.f1
        );
    }
}
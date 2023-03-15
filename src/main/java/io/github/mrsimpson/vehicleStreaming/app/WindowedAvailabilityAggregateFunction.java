package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.ParkingInterval;
import io.github.mrsimpson.vehicleStreaming.util.ParkingIntervalTuple;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

import java.time.Instant;

public class WindowedAvailabilityAggregateFunction implements AggregateFunction<ParkingIntervalTuple, Tuple4<String, Long, Instant, Instant>, Tuple2<String, Double>> {
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
    public Tuple4<String, Long, Instant, Instant> createAccumulator() {
        return new Tuple4<String, Long, Instant, Instant>("", 0L, null, null);
    }

    /**
     * The actual aggregation happens here
     * @param parkingIntervalTuple: the interval detected for a vehicle
     * @param acc: the previous availability for vehicles of this provider within the window
     * @return the updated availability for vehicles of this provider within the window
     */
    @Override
    public Tuple4<String, Long, Instant, Instant> add(ParkingIntervalTuple parkingIntervalTuple, Tuple4<String, Long, Instant, Instant> acc) {
        ParkingInterval parkingInterval = parkingIntervalTuple.f1;
        Instant start = Instant.from(parkingInterval.resolution.subtractFrom(parkingInterval.scheduledEnd.toInstant()));
        return new Tuple4<>(
                parkingInterval.provider,
                parkingInterval.getParkingDurationMillis(),
                (acc.f2 != null && acc.f2.isBefore(start)) ? acc.f2 : start,
                (acc.f3 != null && acc.f3.isAfter(parkingInterval.scheduledEnd.toInstant()) ) ? acc.f3 : parkingInterval.scheduledEnd.toInstant()
        );
    }

    /**
     * This method is being called at the end of the window. Its result will be emitted downstream
     * @param acc: The accumulation aggregated during the window
     * @return the total available ratio based on the time window's size
     */
    @Override
    public Tuple2<String, Double> getResult(Tuple4<String, Long, Instant, Instant> acc) {

        return new Tuple2<>(acc.f0, acc.f1.doubleValue() / (acc.f3.toEpochMilli() - acc.f2.toEpochMilli()) );
    }

    /**
     * This is being called if two intermediate states (no matter how they come into existence though) get aggregated
     * @param acc1: The first intermediate state
     * @param acc2: The second intermediate state
     * @return The merged state
     */
    @Override
    public Tuple4<String, Long, Instant, Instant> merge(Tuple4<String, Long, Instant, Instant> acc1, Tuple4<String, Long, Instant, Instant> acc2) {
        return new Tuple4<>(
                acc1.f0,
                acc1.f1 + acc2.f1,
                (acc1.f2 != null && acc1.f2.isBefore(acc2.f2)) ? acc1.f2 : acc2.f2,
                (acc1.f3 != null && acc1.f3.isAfter(acc2.f3)) ? acc1.f3 : acc2.f3
        );
    }
}
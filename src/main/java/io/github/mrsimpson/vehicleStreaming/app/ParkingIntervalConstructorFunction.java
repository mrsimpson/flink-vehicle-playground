package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.ParkingInterval;
import io.github.mrsimpson.vehicleStreaming.util.ParkingIntervalTuple;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Date;

/**
 * Generates a  parking interval for a vehicle with a given resolution upon completion.
 * A parking interval is considered "completed" if either the duration of the resolution passed
 * or if an event making the vehicle disappear was recorded (for simplification, we'll consider only TRIP_START here)
 */
public class ParkingIntervalConstructorFunction extends KeyedProcessFunction<String, VehicleEvent, ParkingIntervalTuple> {
    public static final OutputTag<Error> ERROR_OUTPUT_TAG = new OutputTag<>("parking-errors") {
    };

    private final Duration resolution;

    // function state
    private transient ValueState<ParkingInterval> previousInterval;

    public ParkingIntervalConstructorFunction(Duration resolution) {
        this.resolution = resolution;
    }

    // helpers
    static Date getNextIntervalEnd(Date startDate, Duration resolution) {
        if (resolution.compareTo(Duration.ofDays(1)) >= 0)
            throw new Error("Only resolutions shorter than a day possible");
        Date startOfDay = (Date) startDate.clone();
        startOfDay.setTime(0);

        Duration sinceToday = Duration.between(startOfDay.toInstant(), startDate.toInstant());
        long passedIntervalsToday = sinceToday.dividedBy(resolution);

        return new Date(startOfDay.toInstant().plus(resolution.multipliedBy(passedIntervalsToday + 1)).toEpochMilli());//  Date.from(nextFutureDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }


    @Override
    public void processElement(VehicleEvent vehicleEvent, Context ctx, Collector<ParkingIntervalTuple> out) throws Exception {

        // as state, we need to remember the vehicle's previous interval
        ParkingInterval current = previousInterval.value();

        switch (vehicleEvent.type) {
            case TRIP_START:
                if (current == null) {
                    return; // do nothing. we didn't record this vehicle as parked earlier
                }

                // complete the parking interval
                current.terminate(vehicleEvent.eventTime);
                // remove potential timers which have been created earlier
                ctx.timerService().deleteEventTimeTimer(current.scheduledEnd.getTime());

                // publish it
                out.collect(new ParkingIntervalTuple(ctx.getCurrentKey(), current));
                previousInterval.update(null);

                break;
            case TRIP_END:
                current = new ParkingInterval(
                        vehicleEvent.id,
                        vehicleEvent.provider,
                        vehicleEvent.location,
                        vehicleEvent.eventTime,
                        this.resolution);

                // schedule a timer to complete the current interval once the current resolution interval is over
                current.scheduleEnd(getNextIntervalEnd(current.start, this.resolution));
                ctx.timerService().registerEventTimeTimer(current.scheduledEnd.getTime());

                // publish it
                out.collect(new ParkingIntervalTuple(ctx.getCurrentKey(), current));

                previousInterval.update(current);
                break;
            default:
                return; //ignore all other events
        }
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<ParkingInterval> valueStateDescriptor = new ValueStateDescriptor<>("previousInterval", ParkingInterval.class);
        previousInterval = getRuntimeContext().getState(valueStateDescriptor);
    }
}

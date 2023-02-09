package io.github.mrsimpson.vehicleStreaming.util;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Date;
import java.util.Random;

/**
 * Flink SourceFunction to generate Vehicle Events
 * <p>
 * Each parallel instance of the source simulates a configurable number of vehicles emitting events
 * <p>
 * Note: This is a simple data-generating source function that does not checkpoint its state.
 * In case of a failure, the source does not replay any data.
 */
public class VehicleEventsGenerator extends RichParallelSourceFunction<VehicleEvent> {

    // flag indicating whether source is still running
    private boolean running = true;

    // make the load configurable
    private int fleetSize = 10;
    private int frequency = 1000;

    private static final double ONE_HUNDRED_M = 0.0008993;
    private  static final long THIRTY_SECONDS = 30_000;

    public VehicleEventsGenerator(int fleetSize, int frequency) {
        this.fleetSize = fleetSize;
        this.frequency = frequency;
    }

    // poor developer's state machine
    // @see https://github.com/openmobilityfoundation/mobility-data-specification/blob/main/general-information.md#state-machine-diagram
    private VehicleStateType determineNextState(VehicleStateType currentState, VehicleEventType eventType) {
        switch (eventType) {
            case TRIP_START:
                return VehicleStateType.ON_TRIP;
            case TRIP_END:
                return VehicleStateType.AVAILABLE;
            case LOCATED:
            default:
                return currentState;
        }
    }

    // some random events generation which is compatible to the state machine
    private VehicleEventType determineNextEventType(VehicleStateType currentState) {
        Random rand = new Random();

        switch (currentState) {
            case UNKNOWN: // initial state
                if (rand.nextDouble() > 0.5) {
                    return VehicleEventType.TRIP_START;
                } else {
                    return VehicleEventType.TRIP_END;
                }
            case ON_TRIP: // how likely is it a trip is going to stop (each 30s)
                if (rand.nextDouble() > 0.95) {
                    return VehicleEventType.TRIP_END;
                } else {
                    return VehicleEventType.LOCATED;
                }
            case AVAILABLE: // how likely is it a trip is going to start (each 30s)
                if (rand.nextDouble() > 0.95) {
                    return VehicleEventType.TRIP_START;
                } else {
                    return VehicleEventType.LOCATED;
                }
            default:
                return null;
        }
    }

    /**
     * run() continuously emits Vehicle Events by emitting them through the SourceContext.
     */
    @Override
    public void run(SourceContext<VehicleEvent> srcCtx) throws Exception {

        // initialize random number generator
        Random rand = new Random();

        // Start with even time of now, adding 30s per interval.
        // within this duration, each vehicle will move between 0 and sqrt(100^2 + 100^2) meters
        Date eventTime = new Date();

        // look up index of this parallel task in order to produce unique IDs
        int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();

        // initialize vehicles and their locations
        String[] vehicleIds = new String[fleetSize];
        double[] lats = new double[fleetSize];
        double[] longs = new double[fleetSize];
        VehicleStateType[] state = new VehicleStateType[fleetSize];

        // set up vehicles somewhere in Frankfurt
        for (int i = 0; i < fleetSize; i++) {
            vehicleIds[i] = "vehicle_" + (taskIdx * fleetSize + i);
            // define an area in which vehicles shall move
            double maxLat = 50.15369548027726;
            double minLat = 50.099109980321764;
            lats[i] = (minLat + maxLat)/2 + (maxLat - minLat) * (rand.nextDouble() - 0.5);

            double maxLong = 8.736120357648904;
            double minLong = 8.604971075298;
            longs[i] = (minLong + maxLong)/2 + (maxLong - minLong) * (rand.nextDouble() - 0.5);

            state[i] = VehicleStateType.UNKNOWN;
        }

        while (running) {

            // produce new vehicle events
            for (int i = 0; i < fleetSize; i++) {

                // invoke state transition
                VehicleStateType currentState = state[i];
                VehicleEventType eventType = determineNextEventType(currentState);
                if(eventType != null) {
                    state[i] = determineNextState(currentState, eventType);

                    //determine a new location
                    lats[i] += (rand.nextDouble() - 0.5) * ONE_HUNDRED_M;
                    longs[i] += (rand.nextDouble() - 0.5) * ONE_HUNDRED_M;

                    // emit the event
                    srcCtx.collect(new VehicleEvent(
                            vehicleIds[i],
                            eventTime,
                            "provider_" + taskIdx,
                            new Location(lats[i], longs[i]),
                            eventType,
                            state[i])
                    );
                }
            }

            Thread.sleep(frequency);
            eventTime.setTime(eventTime.getTime() + THIRTY_SECONDS);
        }
    }

    /**
     * Cancels this SourceFunction.
     */
    @Override
    public void cancel() {
        this.running = false;
    }
}

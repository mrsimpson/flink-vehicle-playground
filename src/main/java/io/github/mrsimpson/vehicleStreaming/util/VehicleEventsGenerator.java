package io.github.mrsimpson.vehicleStreaming.util;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

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

    public VehicleEventsGenerator(int fleetSize, int frequency) {
        this.fleetSize = fleetSize;
        this.frequency = frequency;
    }

    /**
     * run() continuously emits Vehicle Events by emitting them through the SourceContext.
     */
    @Override
    public void run(SourceContext<VehicleEvent> srcCtx) throws Exception {

        // initialize random number generator
        Random rand = new Random();

        // look up index of this parallel task in order to produce unique IDs
        int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();

        // initialize vehicles and their locations
        String[] vehicleIds = new String[fleetSize];
        double[] lats = new double[fleetSize];
        double[] longs = new double[fleetSize];
        for (int i = 0; i < fleetSize; i++) {
            vehicleIds[i] = "vehicle_" + (taskIdx * fleetSize + i);
            // define an area in which vehicles shall move
            double maxLat = 50.21423506422072;
            double minLat = 50.022714207807326;
            lats[i] = minLat + (maxLat - minLat) * rand.nextGaussian();
            double maxLong = 8.80513526424458;
            double minLong = 8.469365626706495;
            longs[i] = minLong + (maxLong - minLong) * rand.nextGaussian();
        }

        while (running) {

            // emit SensorReadings
            for (int i = 0; i < fleetSize; i++) {

                //determine a new location
                lats[i] += (rand.nextGaussian() - 0.5) * ONE_HUNDRED_M;
                longs[i] += (rand.nextGaussian() - 0.5) * ONE_HUNDRED_M;
                // emit reading
                srcCtx.collect(new VehicleEvent(vehicleIds[i], lats[i], longs[i], VehicleEventType.LOCATED, VehicleStateType.AVAILABLE));
            }

            Thread.sleep(frequency);
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

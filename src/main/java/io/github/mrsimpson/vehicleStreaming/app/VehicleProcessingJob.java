package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEventsGenerator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class VehicleProcessingJob {

    static int fleetSize = 1;
    static int frequency = 1000;

    static int numberOfProviders = 1;

    public static void main(String[] args) throws Exception {
        // configure Job based on arguments – poor man's cli
        if (args.length > 0) {
            int arg1 = Integer.parseInt(args[0]);
            if (arg1 > 0) {
                fleetSize = arg1;
            }
        }

        if (args.length > 1) {
            int arg2 = Integer.parseInt(args[1]);
            if (arg2 > 0) {
                frequency = arg2;
            }
        }

        if (args.length > 2) {

            int arg3 = Integer.parseInt(args[2]);
            if (arg3 > 0) {
                numberOfProviders = arg3;
            }
        }

        Logger.getLogger("stdout").log(new LogRecord(Level.WARNING, "Läuft " + new Date()));

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);

        RichParallelSourceFunction<VehicleEvent> events = new VehicleEventsGenerator(fleetSize, frequency);

        // Set up the application based on the context (sources and sinks)
        VehicleStreamingPipeline app = new VehicleStreamingPipelineBuilder()
                .setEnv(env)
                .setVehicleEvents(events)
                .setRentalsCountSink(new PrintSinkFunction<>("Rentals", false))
                .setReturnsCountSink(new PrintSinkFunction<>("Returns", false))
                .setTripSink(new PrintSinkFunction<>("Trips", false))
                .createVehicleStreamingPipeline();

        app.run(numberOfProviders);
    }
}

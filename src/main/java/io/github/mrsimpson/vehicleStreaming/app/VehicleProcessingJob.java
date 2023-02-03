package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEventsGenerator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.commons.cli.*;

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class VehicleProcessingJob {

    static int fleetSize = 1;
    static int frequency = 1000;

    static int numberOfProviders = 1;

    public static void main(String[] args) throws Exception {
        // configure Job based on arguments
        Options options = new Options();
        options.addOption("fs", "fleetsize", true, "Size of each provider's fleet");
        options.addOption("fr", "frequency", true, "How fast shall 1min event time pass");
        options.addOption("p","providers", true, "Number of provider");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("fleetsize")) {
            int fleetSizeArg = Integer.parseInt(cmd.getOptionValue("fleetsize"));
            if (fleetSizeArg > 0) {
                fleetSize = fleetSizeArg;
            }
        }
        if (cmd.hasOption("frequency")) {
            int frequencyArg = Integer.parseInt(cmd.getOptionValue("frequency"));
            if (frequencyArg > 0) {
                frequency = frequencyArg;
            }
        }

        if (cmd.hasOption("providers")) {
            int providersArg = Integer.parseInt(cmd.getOptionValue("providers"));
            if (providersArg > 0) {
                numberOfProviders = providersArg;
            }
        }

        Logger.getLogger("stdout").log(new LogRecord(Level.WARNING, "Läuft " + new Date()));

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));

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

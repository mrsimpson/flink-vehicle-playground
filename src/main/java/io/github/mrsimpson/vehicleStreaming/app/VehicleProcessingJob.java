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

    public static void main(String[] args) throws Exception {
        Logger.getLogger("stdout").log(new LogRecord(Level.WARNING, "Läuft " + new Date()));

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        RichParallelSourceFunction<VehicleEvent> events = new VehicleEventsGenerator(1, 1000);

        // Set up the application based on the context (sources and sinks)
        VehicleStreamingPipeline app = new VehicleStreamingPipelineBuilder()
                .setEnv(env)
                .setVehicleEvents(events)
                .setRentalsCountSink(new PrintSinkFunction<>("Rentals", false))
                .setReturnsCountSink(new PrintSinkFunction<>("Returns", false))
                .createVehicleStreamingPipeline();

        app.run();
    }
}

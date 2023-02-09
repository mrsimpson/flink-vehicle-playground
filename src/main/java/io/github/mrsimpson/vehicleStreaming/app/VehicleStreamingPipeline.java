package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.time.Duration;

/**
 * This pipeline defines the Flink application:
 * It connects streaming inputs with operators and dumps them to sinks
 * All dependencies are injected via constructor injection
 */
public class VehicleStreamingPipeline {
    private final StreamExecutionEnvironment env;
    private final RichParallelSourceFunction<VehicleEvent> vehicleEvents;
    private final Sink<Tuple2<String, Integer>> rentalsCountSink;
    private final Sink<Tuple2<String, Integer>> returnsCountSink;
    private final Sink<TripTuple> tripSink;

    private final Sink<VehicleEvent> rawVehicleEventsSink;
    private final Sink<Error> errorStreamSink;

    // Constructor injection for basic Unit Testing Support
    VehicleStreamingPipeline(StreamExecutionEnvironment env,
                             RichParallelSourceFunction<VehicleEvent> vehicleEvents,
                             Sink<Tuple2<String, Integer>> rentalsCountSink,
                             Sink<Tuple2<String, Integer>> returnsCountSink,
                             Sink<TripTuple> tripSink,
                             Sink<VehicleEvent> rawVehicleEventsSink
    ) {
        this.env = env;
        this.vehicleEvents = vehicleEvents;
        this.rawVehicleEventsSink = (rawVehicleEventsSink != null) ? rawVehicleEventsSink : new NullSink<>();
        this.rentalsCountSink = (rentalsCountSink != null) ? rentalsCountSink : new NullSink<>();
        this.returnsCountSink = (returnsCountSink != null) ? returnsCountSink : new NullSink<>();
        this.tripSink = (tripSink != null) ? tripSink : new NullSink<>();
        this.errorStreamSink = new PrintSink<>(true);
    }

    public void run(int numberOfProviders) throws Exception {

        SingleOutputStreamOperator<VehicleEvent> stream = this.env
                .addSource(this.vehicleEvents)
                .setParallelism(numberOfProviders)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                            .forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner(new VehicleEventsTimestampAssigner())
                );
//                .assignTimestampsAndWatermarks(new VehicleEventsTimerAssigner()); // deprecated
        stream
                .name("raw-vehicle-events")
                .sinkTo(this.rawVehicleEventsSink);

        SingleOutputStreamOperator<Tuple2<String, Integer>> rentalsCountStream =
                stream
                        .filter(v -> v.type == VehicleEventType.TRIP_START)
                        .keyBy(v -> v.provider)
                        .process(new CountFunction())
                        .name("rentals-stream");

        rentalsCountStream
                .sinkTo(this.rentalsCountSink)
                .name("rentals-count-sink");

        SingleOutputStreamOperator<Tuple2<String, Integer>> returnsCountStream =
                stream
                        .filter(v -> v.type == VehicleEventType.TRIP_END)
                        .keyBy(v -> v.provider)
                        .process(new CountFunction())
                        .name("returns-stream");

        returnsCountStream
                .sinkTo(this.returnsCountSink)
                .name("returns-count-sink");

        SingleOutputStreamOperator<TripTuple> tripsStream =
                stream
                        .keyBy(v -> v.id)
                        .process(new TripConstructorFunction())
                        .name("trips-stream");

        tripsStream
                .sinkTo(this.tripSink)
                .name("trips-sink");

        // propagate errors from side output
        tripsStream
                .getSideOutput(TripConstructorFunction.ERROR_OUTPUT_TAG)
                .sinkTo(this.errorStreamSink)
                .name("error-stream-sink");

        // Execute the pipeline
        this.env.execute("Vehicle Events processing");
    }
}

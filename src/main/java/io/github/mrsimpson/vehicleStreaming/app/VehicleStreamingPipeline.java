package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.NullSink;
import io.github.mrsimpson.vehicleStreaming.util.Trip;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEventType;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
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
    private final SinkFunction<Tuple2<String, Integer>> rentalsCountSink;
    private final SinkFunction<Tuple2<String, Integer>> returnsCountSink;
    private final SinkFunction<Tuple2<String, Trip>> tripSink;

    private final SinkFunction<VehicleEvent> rawVehicleEventsSink;
    private final SinkFunction<Error> errorStreamSink;

    // Constructor injection for basic Unit Testing Support
    VehicleStreamingPipeline(StreamExecutionEnvironment env,
                             RichParallelSourceFunction<VehicleEvent> vehicleEvents,
                             SinkFunction<Tuple2<String, Integer>> rentalsCountSink,
                             SinkFunction<Tuple2<String, Integer>> returnsCountSink,
                             SinkFunction<Tuple2<String, Trip>> tripSink, SinkFunction<VehicleEvent> rawVehicleEventsSink
    ) {
        this.env = env;
        this.vehicleEvents = vehicleEvents;
        this.rawVehicleEventsSink = (rawVehicleEventsSink != null) ? rawVehicleEventsSink : new PrintSinkFunction<>();
        this.rentalsCountSink = (rentalsCountSink != null) ? rentalsCountSink : new NullSink<>();
        this.returnsCountSink = (returnsCountSink != null) ? returnsCountSink : new NullSink<>();
        this.tripSink = (tripSink != null) ? tripSink : new NullSink<>();
        this.errorStreamSink = new PrintSinkFunction<>(true);
    }

    public void run(int numberOfProviders) throws Exception {

        SingleOutputStreamOperator<VehicleEvent> stream = this.env
                .addSource(this.vehicleEvents)
                .setParallelism(numberOfProviders)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                            .forBoundedOutOfOrderness(Duration.ofMinutes(1))
                                .withTimestampAssigner(new VehicleEventsTimestampAssignerSupplier())
                );
//                .assignTimestampsAndWatermarks(new VehicleEventsTimerAssigner()); // deprecated
        stream.addSink(rawVehicleEventsSink).name("raw-vehicle-events");

        SingleOutputStreamOperator<Tuple2<String, Integer>> rentalsCountStream =
                stream
                        .filter(v -> v.type == VehicleEventType.TRIP_START)
                        .keyBy(v -> v.provider)
                        .process(new CountFunction())
                        .name("rentals-stream");

        rentalsCountStream
                .addSink(this.rentalsCountSink)
                .name("rentals-count-sink");

        SingleOutputStreamOperator<Tuple2<String, Integer>> returnsCountStream =
                stream
                        .filter(v -> v.type == VehicleEventType.TRIP_END)
                        .keyBy(v -> v.provider)
                        .process(new CountFunction())
                        .name("returns-stream");

        returnsCountStream
                .addSink(this.returnsCountSink)
                .name("returns-count-sink");

        SingleOutputStreamOperator<Tuple2<String, Trip>> tripsStream =
                stream
                        .keyBy(v -> v.id)
                        .process(new TripConstructorFunction())
                        .name("trips-stream");

        tripsStream
                .addSink(this.tripSink)
                .name("trips-sink");

        // propagate errors from side output
        tripsStream
                .getSideOutput(TripConstructorFunction.ERROR_OUTPUT_TAG)
                .addSink(this.errorStreamSink)
                .name("error-stream-sink");

        // Execute the pipeline
        this.env.execute("Vehicle Events processing");
    }
}

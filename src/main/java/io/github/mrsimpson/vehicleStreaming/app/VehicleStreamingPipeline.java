package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.NullSink;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEventType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

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

    private final SinkFunction<VehicleEvent> rawVehicleEventsSink;

    // Constructor injection for basic Unit Testing Support
    VehicleStreamingPipeline(StreamExecutionEnvironment env,
                             RichParallelSourceFunction<VehicleEvent> vehicleEvents,
                             SinkFunction<Tuple2<String, Integer>> rentalsCountSink,
                             SinkFunction<Tuple2<String, Integer>> returnsCountSink,
                             SinkFunction<VehicleEvent> rawVehicleEventsSink
                             ) {
        this.env = env;
        this.vehicleEvents = vehicleEvents;
        this.rentalsCountSink = (rentalsCountSink != null) ? rentalsCountSink : new NullSink<>();
        this.returnsCountSink = (returnsCountSink != null) ? returnsCountSink : new NullSink<>();
        this.rawVehicleEventsSink = (rawVehicleEventsSink != null) ? rawVehicleEventsSink : new PrintSinkFunction<>();
    }

    public void run(int parallelism) throws Exception {

        DataStreamSource<VehicleEvent> stream = this.env
                .addSource(this.vehicleEvents)
                .setParallelism(parallelism);
        stream.addSink(rawVehicleEventsSink);

        DataStream<org.apache.flink.api.java.tuple.Tuple2<String, Integer>> rentalsCountStream =
                stream
                        .filter(v -> v.type == VehicleEventType.TRIP_START)
                        .keyBy(v -> v.provider)
                        .process(new CountFunction());

        rentalsCountStream
                .addSink(this.rentalsCountSink);

        DataStream<org.apache.flink.api.java.tuple.Tuple2<String, Integer>> returnsCountStream =
                stream
                        .filter(v -> v.type == VehicleEventType.TRIP_END)
                        .keyBy(v -> v.provider)
                        .process(new CountFunction());

        returnsCountStream
                .addSink(this.returnsCountSink);

        // Execute the pipeline
        this.env.execute("Vehicle Events processing");
    }
}

package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEventType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class VehicleStreamingPipeline {
    private final StreamExecutionEnvironment env;
    private final RichParallelSourceFunction<VehicleEvent> vehicleEvents;
    private final SinkFunction<Tuple2<String, Integer>> rentalsCountSink;
    private final SinkFunction<Tuple2<String, Integer>> returnsCountSink;

    // Constructor injection for basic Unit Testing Support
    VehicleStreamingPipeline(StreamExecutionEnvironment env, RichParallelSourceFunction<VehicleEvent> vehicleEvents, SinkFunction<Tuple2<String, Integer>> rentalsCountSink, SinkFunction<Tuple2<String, Integer>> returnsCountSink) {
        this.env = env;
        this.vehicleEvents = vehicleEvents;
        this.rentalsCountSink = rentalsCountSink;
        this.returnsCountSink = returnsCountSink;
    }

    public void run() throws Exception {

        DataStreamSource<VehicleEvent> stream = this.env
                .addSource(this.vehicleEvents)
                .setParallelism(1);

        stream.print();

        DataStream<org.apache.flink.api.java.tuple.Tuple2<String, Integer>> rentalsCountStream =
                stream
                        .filter(v -> v.type == VehicleEventType.TRIP_START)
                        .keyBy(v -> v.id).process(new CountFunction());

        rentalsCountStream
                .addSink(this.rentalsCountSink);

        DataStream<org.apache.flink.api.java.tuple.Tuple2<String, Integer>> returnsCountStream =
                stream
                        .filter(v -> v.type == VehicleEventType.TRIP_END)
                        .keyBy(v -> v.id).process(new CountFunction());

        returnsCountStream
                .addSink(this.returnsCountSink);

        this.env.execute("Vehicle Events processing");
    }
}

package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class VehicleStreamingPipelineBuilder {
    private StreamExecutionEnvironment env;
    private RichParallelSourceFunction<VehicleEvent> vehicleEvents;
    private SinkFunction<Tuple2<String, Integer>> rentalsCountSink;
    private SinkFunction<Tuple2<String, Integer>> returnsCountSink;
    private SinkFunction<VehicleEvent> rawVehicleEventsSink;

    public VehicleStreamingPipelineBuilder setEnv(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public VehicleStreamingPipelineBuilder setVehicleEvents(RichParallelSourceFunction<VehicleEvent> vehicleEvents) {
        this.vehicleEvents = vehicleEvents;
        return this;
    }

    public VehicleStreamingPipelineBuilder setRentalsCountSink(SinkFunction<Tuple2<String, Integer>> rentalsCountSink) {
        this.rentalsCountSink = rentalsCountSink;
        return this;
    }

    public VehicleStreamingPipelineBuilder setReturnsCountSink(SinkFunction<Tuple2<String, Integer>> returnsCountSink) {
        this.returnsCountSink = returnsCountSink;
        return this;
    }

    public VehicleStreamingPipelineBuilder setRawVehicleEventsSink(SinkFunction<VehicleEvent> rawVehicleEventsSink) {
        this.rawVehicleEventsSink = rawVehicleEventsSink;
        return this;
    }

    public VehicleStreamingPipeline createVehicleStreamingPipeline() {
        return new VehicleStreamingPipeline(env, vehicleEvents, rentalsCountSink, returnsCountSink, rawVehicleEventsSink);
    }
}
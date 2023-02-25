package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.ParkingIntervalTuple;
import io.github.mrsimpson.vehicleStreaming.util.TripTuple;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class VehicleStreamingPipelineBuilder {
    private StreamExecutionEnvironment env;
    private RichParallelSourceFunction<VehicleEvent> vehicleEvents;
    private Sink<Tuple2<String, Integer>> rentalsCountSink;
    private Sink<Tuple2<String, Integer>> returnsCountSink;
    private Sink<VehicleEvent> rawVehicleEventsSink;
    private Sink<TripTuple> tripSink;
    private Sink<ParkingIntervalTuple> parkingSink;

    public VehicleStreamingPipelineBuilder setEnv(StreamExecutionEnvironment env) {
        this.env = env;
        return this;
    }

    public VehicleStreamingPipelineBuilder setVehicleEvents(RichParallelSourceFunction<VehicleEvent> vehicleEvents) {
        this.vehicleEvents = vehicleEvents;
        return this;
    }

    public VehicleStreamingPipelineBuilder setRentalsCountSink(Sink<Tuple2<String, Integer>> rentalsCountSink) {
        this.rentalsCountSink = rentalsCountSink;
        return this;
    }

    public VehicleStreamingPipelineBuilder setReturnsCountSink(Sink<Tuple2<String, Integer>> returnsCountSink) {
        this.returnsCountSink = returnsCountSink;
        return this;
    }

    public VehicleStreamingPipelineBuilder setTripSink(Sink<TripTuple> tripSink) {
        this.tripSink = tripSink;
        return this;
    }

    public VehicleStreamingPipelineBuilder setParkingSink(Sink<ParkingIntervalTuple> parkingSink) {
        this.parkingSink = parkingSink;
        return this;
    }

    public VehicleStreamingPipelineBuilder setRawVehicleEventsSink(Sink<VehicleEvent> rawVehicleEventsSink) {
        this.rawVehicleEventsSink = rawVehicleEventsSink;
        return this;
    }

    public VehicleStreamingPipeline createVehicleStreamingPipeline() {
        return new VehicleStreamingPipeline(env, vehicleEvents, rentalsCountSink, returnsCountSink, tripSink, parkingSink, rawVehicleEventsSink);
    }
}
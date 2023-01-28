package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEventsGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class VehicleStreamingApp {
    private final StreamExecutionEnvironment env;
    private final VehicleEventsGenerator vehicleEvents;
    private final RichSinkFunction<Tuple2<String, Integer>> countSink;

    // Constructor injection for basic Unit Testing Support
    VehicleStreamingApp(StreamExecutionEnvironment env, VehicleEventsGenerator vehicleEvents, RichSinkFunction<Tuple2<String, Integer>> countSink){
        this.env = env;
        this.vehicleEvents = vehicleEvents;
        this.countSink = countSink;
    }

    public void run() throws Exception {

        DataStreamSource<VehicleEvent> stream = this.env
                .addSource(this.vehicleEvents)
                .setParallelism(1);

        KeyedStream<VehicleEvent, String> eventsByVehicle = stream.keyBy(v -> v.id);

        DataStream<org.apache.flink.api.java.tuple.Tuple2<String,Integer>> countStream = eventsByVehicle
                .map(new MapFunction<VehicleEvent, org.apache.flink.api.java.tuple.Tuple2<String, Integer>>() {
                    @Override
                    public org.apache.flink.api.java.tuple.Tuple2<String, Integer> map(VehicleEvent v){
                        return new org.apache.flink.api.java.tuple.Tuple2<>(v.id, 1);
                    }
                })
                .keyBy(0)
                .sum(1);

        countStream.addSink(this.countSink);

        /* I'd actually like to be able to inject a local DataStreamSource as well, but ... types don't matchx
        DataStreamSource<VehicleEvent> vehicleEvents = env.fromElements(
                new VehicleEvent("1", 8.6819631,50.1107767),
                new VehicleEvent("2", 8.6833854,50.1109276),
                new VehicleEvent("1", 8.6849040,50.1108864),
                new VehicleEvent("1", 8.6849575,50.1102760),
                new VehicleEvent("2", 8.6835459,50.1099743),
                new VehicleEvent("1", 8.6817064,50.1090964)
        );
        VehicleStreamingApp app = new VehicleStreamingApp(env, vehicleEvents, new PrintSinkFunction<Tuple2<String, Integer>>());
        */
        this.env.execute("Vechile Events processing");
    }

}

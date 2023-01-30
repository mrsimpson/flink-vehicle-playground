package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.Tuple2TestSinkFunction;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEventType;
import io.github.mrsimpson.vehicleStreaming.util.VehicleStateType;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;


public class VehicleStreamingPipelineTest {

    @Test
    public void e2e() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        // Set up
        RichParallelSourceFunction<VehicleEvent> events = new RichParallelSourceFunction<VehicleEvent>() {
            @Override
            public void run(SourceContext<VehicleEvent> ctx) {
                ctx.collect(new VehicleEvent("1", 8.6819631, 50.1107767, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP));
                ctx.collect(new VehicleEvent("1", 8.6849040, 50.1108864, VehicleEventType.TRIP_END, VehicleStateType.AVAILABLE));
                ctx.collect(new VehicleEvent("1", 8.6849040, 50.1108864, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP));
                ctx.collect(new VehicleEvent("2", 8.6833854, 50.1109276, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP));
                ctx.collect(new VehicleEvent("1", 8.6849575, 50.1102760, VehicleEventType.LOCATED, VehicleStateType.ON_TRIP));
                ctx.collect(new VehicleEvent("2", 8.6835459, 50.1099743, VehicleEventType.TRIP_END, VehicleStateType.AVAILABLE));
                ctx.collect(new VehicleEvent("1", 8.6817064, 50.1090964, VehicleEventType.TRIP_END, VehicleStateType.AVAILABLE));
            }

            @Override
            public void cancel() {

            }
        };

        Tuple2TestSinkFunction countSink = new Tuple2TestSinkFunction();
        countSink.clear();
        VehicleStreamingPipeline app = new VehicleStreamingPipeline(env, events, countSink);

        // Invoke function under test
        app.run();

        // Validate result
        List<Tuple2<String, Integer>> result = countSink.getCollectedElements();
        Assert.assertEquals(7, result.size());
        Assert.assertTrue(result.contains(Tuple2.of("1", 5)));
        Assert.assertTrue(result.contains(Tuple2.of("2", 2)));
    }
}

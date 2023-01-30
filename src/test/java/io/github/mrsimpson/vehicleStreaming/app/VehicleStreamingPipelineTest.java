package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEventType;
import io.github.mrsimpson.vehicleStreaming.util.VehicleStateType;
import org.apache.commons.collections.list.SynchronizedList;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;


public class VehicleStreamingPipelineTest {
    private static class CountSinkMock implements SinkFunction<Tuple2<String, Integer>> {

        // Since the Sink set up during th graph construction is another instance than the one used when running the job,
        // the buffer during the test needs to be static
        // @see https://anton-bakalets.medium.com/flink-job-unit-testing-df4f618d07a6
        // @see https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/testing/#testing-flink-jobs
        private static final List<Tuple2<String, Integer>> collectedElements =  Collections.synchronizedList(new ArrayList<Tuple2<String, Integer>>());
        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) {
            collectedElements.add(value);
        }

        public static List<Tuple2<String, Integer>> getCollectedElements() {
            return collectedElements;
        }

        public static void clear(){
            collectedElements.clear();
        }
    }
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

        // Invoke function under test
        VehicleStreamingPipeline app = new VehicleStreamingPipeline(env, events, new CountSinkMock());
        app.run();

        // Validate result
        List<Tuple2<String, Integer>> result = CountSinkMock.getCollectedElements();
        Assert.assertEquals(7, result.size());
        Assert.assertTrue(result.contains(Tuple2.of("1", 5)));
        Assert.assertTrue(result.contains(Tuple2.of("2", 2)));
    }
}
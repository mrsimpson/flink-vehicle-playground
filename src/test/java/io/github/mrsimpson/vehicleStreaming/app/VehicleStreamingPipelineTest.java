package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.*;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class VehicleStreamingPipelineTest {
    private static class CountSinkMock implements Sink<Tuple2<String, Integer>> {

        // Since the Sink set up during the graph construction is another instance than the one used when running the job,
        // the buffer during the test needs to be static
        // @see https://anton-bakalets.medium.com/flink-job-unit-testing-df4f618d07a6
        // @see https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/testing/#testing-flink-jobs
        private static final List<Tuple2<String, Integer>> collectedElements = Collections.synchronizedList(new ArrayList<>());

        public static List<Tuple2<String, Integer>> getCollectedElements() {
            return collectedElements;
        }

        public static void clear() {
            collectedElements.clear();
        }

        @Override
        public SinkWriter<Tuple2<String, Integer>> createWriter(InitContext initContext) {
            return new SinkWriter<>() {

                @Override
                public void close() {

                }

                @Override
                public void write(Tuple2<String, Integer> tuple, Context context) {
                    collectedElements.add(tuple);
                }

                @Override
                public void flush(boolean b) {
                }
            };
        }
    }

    @Test
    public void integration() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        // Set up
        RichParallelSourceFunction<VehicleEvent> events = new RichParallelSourceFunction<>() {
            @Override
            public void run(SourceContext<VehicleEvent> ctx) {
                ctx.collect(new VehicleEvent("1", "provider_1", TestLocations.A, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP));
                ctx.collect(new VehicleEvent("1", "provider_1", TestLocations.B, VehicleEventType.TRIP_END, VehicleStateType.AVAILABLE));
                ctx.collect(new VehicleEvent("1", "provider_1", TestLocations.B, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP));
                ctx.collect(new VehicleEvent("2", "provider_1", TestLocations.A, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP));
                ctx.collect(new VehicleEvent("1", "provider_1", TestLocations.D, VehicleEventType.LOCATED, VehicleStateType.ON_TRIP));
                ctx.collect(new VehicleEvent("2", "provider_1", TestLocations.C, VehicleEventType.TRIP_END, VehicleStateType.AVAILABLE));
                ctx.collect(new VehicleEvent("1", "provider_1", TestLocations.E, VehicleEventType.TRIP_END, VehicleStateType.AVAILABLE));

                // emit two additional TRIP-START-Events in order to clear timers set up for ongoing parkings.
                // This is definitely not how it's supposed to be, but I found not other option how to cancel an existing timer
                ctx.collect(new VehicleEvent("2", "provider_1", TestLocations.C, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP));
                ctx.collect(new VehicleEvent("1", "provider_1", TestLocations.E, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP));
            }

            @Override
            public void cancel() {

            }
        };

        // Set up mocks
        CountSinkMock.clear();

        // Invoke function under test
        VehicleStreamingPipeline app = new VehicleStreamingPipelineBuilder()
                .setEnv(env)
                .setVehicleEvents(events)
                .setRentalsCountSink(new CountSinkMock())
                .setRawVehicleEventsSink(new NullSink<>())
                .createVehicleStreamingPipeline();
        app.run(1);

        // Validate result
        List<Tuple2<String, Integer>> result = CountSinkMock.getCollectedElements();
        Assert.assertEquals(5, result.size()); // total rentals emitted – ignores the key!
        Assert.assertTrue(result.contains(Tuple2.of("provider_1", 5)));
    }
}

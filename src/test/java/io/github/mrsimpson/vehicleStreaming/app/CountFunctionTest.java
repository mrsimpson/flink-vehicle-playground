package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import io.github.mrsimpson.vehicleStreaming.util.VehicleEventType;
import io.github.mrsimpson.vehicleStreaming.util.VehicleStateType;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.Assert;
import org.junit.Test;

/**
 * Demo how to test a stateful function operator
 * @see <a href="https://flink.apache.org/news/2020/02/07/a-guide-for-unit-testing-in-apache-flink.html">A Guide for Unit Testing in Apache Flink</a>
 */

public class CountFunctionTest {
    @Test
    public void countInitialEvent() throws Exception {
        CountFunction fut = new CountFunction();
        KeyedOneInputStreamOperatorTestHarness<String, VehicleEvent, Tuple2<String, Integer>> harness = ProcessFunctionTestHarnesses
                .forKeyedProcessFunction(fut, vehicleEvent -> vehicleEvent.provider, Types.STRING);

        harness.processElement(new VehicleEvent("1",
                        "provider_1",
                        8.6819631,
                        50.1107767,
                        VehicleEventType.TRIP_START,
                        VehicleStateType.ON_TRIP),
                1); // This would allow to test out-of-order processing

        Assert.assertEquals("[Record @ 1 : (provider_1,1)]", harness.getOutput().toString());
    }

    @Test
    public void countMultipleEvents() throws Exception {
        CountFunction fut = new CountFunction();
        KeyedOneInputStreamOperatorTestHarness<String, VehicleEvent, Tuple2<String, Integer>> harness = ProcessFunctionTestHarnesses
                .forKeyedProcessFunction(fut, vehicleEvent -> vehicleEvent.provider, Types.STRING);

        harness.processElement(new VehicleEvent("1", "provider_1", 8.6819631, 50.1107767, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP), 1);
        harness.processElement(new VehicleEvent("1", "provider_1", 8.6819631, 50.1107767, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP), 2);
        Assert.assertEquals("[Record @ 1 : (provider_1,1), Record @ 2 : (provider_1,2)]", harness.getOutput().toString());

    }
}

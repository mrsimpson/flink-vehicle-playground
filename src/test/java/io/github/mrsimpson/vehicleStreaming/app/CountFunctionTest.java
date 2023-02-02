package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.Assert;
import org.junit.Test;

/**
 * Demo how to test a stateful function operator
 *
 * @see <a href="https://flink.apache.org/news/2020/02/07/a-guide-for-unit-testing-in-apache-flink.html">A Guide for Unit Testing in Apache Flink</a>
 */

public class CountFunctionTest {
    @Test
    public void countInitialEvent() throws Exception {
        CountFunction fut = new CountFunction();
        KeyedOneInputStreamOperatorTestHarness<String, VehicleEvent, Tuple2<String, Integer>> harness = ProcessFunctionTestHarnesses
                .forKeyedProcessFunction(fut,
                        vehicleEvent -> vehicleEvent.provider,
                        Types.STRING);

        harness.processElement(new VehicleEvent("1", "provider_1", TestLocations.A, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP), 1); // This would allow to test out-of-order processing

        // asserting string equality is only a workaround for creating a deep Objects encapsulating StreamRecords
        Assert.assertEquals("[Record @ 1 : (provider_1,1)]", harness.getOutput().toString());
    }

    @Test
    public void countMultipleProvidersEvents() throws Exception {
        CountFunction fut = new CountFunction();
        KeyedOneInputStreamOperatorTestHarness<String, VehicleEvent, Tuple2<String, Integer>> harness = ProcessFunctionTestHarnesses
                .forKeyedProcessFunction(fut, vehicleEvent -> vehicleEvent.provider, Types.STRING);

        harness.processElement(new VehicleEvent("2", "provider_1", TestLocations.B, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP), 2);
        harness.processElement(new VehicleEvent("3", "provider_2", TestLocations.A, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP), 2);
        harness.processElement(new VehicleEvent("1", "provider_1", TestLocations.A, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP), 1);

        Assert.assertEquals("[Record @ 2 : (provider_1,1), Record @ 2 : (provider_2,1), Record @ 1 : (provider_1,2)]", harness.getOutput().toString());
    }
}

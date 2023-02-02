package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

public class TripConstructorFunctionTest {

    @Test
    public void detectNewTrip() throws Exception {
        TripConstructorFunction fut = new TripConstructorFunction();
        KeyedOneInputStreamOperatorTestHarness<String, VehicleEvent, Tuple2<String, Trip>> harness = ProcessFunctionTestHarnesses
                .forKeyedProcessFunction(
                        fut,
                        vehicleEvent -> vehicleEvent.id,
                        Types.STRING
                );

        Date now = new Date();
        harness.processElement(new VehicleEvent("1", now, "provider_1", TestLocations.A, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP), 1);
        Assert.assertEquals("[Record @ 1 : " + "(1,Trip of 1 started " + new Tracking(now, TestLocations.A) +" ongoing)]"
                , harness.getOutput().toString());    }
}

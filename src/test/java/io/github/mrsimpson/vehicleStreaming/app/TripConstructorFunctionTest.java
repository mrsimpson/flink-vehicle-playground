package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;

public class TripConstructorFunctionTest {

    @Test
    public void detectNewTrip() throws Exception {
        TripConstructorFunction fut = new TripConstructorFunction();
        KeyedOneInputStreamOperatorTestHarness<String, VehicleEvent, Tuple2<String, Trip>> harness = createHarness(fut);
        harness.open();

        Date now = new Date();
        harness.processElement(new VehicleEvent("1", now, "provider_1", TestLocations.A, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP), 1);
        Assert.assertEquals("[Record @ 1 : " + "(1,Trip of 1 started " + new Tracking(now, TestLocations.A) + " ongoing)]"
                , harness.getOutput().toString());

        harness.close();
    }

    @Test
    public void endTrip() throws Exception {
        TripConstructorFunction fut = new TripConstructorFunction();

        // Mock an initial state in which a trip is ongoing
        KeyedOneInputStreamOperatorTestHarness<String, VehicleEvent, Tuple2<String, Trip>> snapshotHarness = createHarness(fut);
        snapshotHarness.open();
        Date startDate = new Date();
        snapshotHarness.processElement(new VehicleEvent("1", startDate, "provider_1", TestLocations.A, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP), 1);
        OperatorSnapshotFinalizer snapshot = snapshotHarness.snapshotWithLocalState(1L, 1L);
        snapshotHarness.close();

        // Create a new harness as container for the actual test execution
        KeyedOneInputStreamOperatorTestHarness<String, VehicleEvent, Tuple2<String, Trip>> testHarness = createHarness(fut);
        testHarness.initializeState(snapshot.getJobManagerOwnedState(), snapshot.getTaskLocalState());
        testHarness.open();

        // perform the actual test
        Date now = new Date();
        testHarness.processElement(new VehicleEvent("1", now, "provider_1", TestLocations.B, VehicleEventType.TRIP_END, VehicleStateType.AVAILABLE), 2);
        Assert.assertEquals("[Record @ 2 : " + "(1,Trip of 1 started " + new Tracking(startDate, TestLocations.A) + " --> " + new Tracking(startDate, TestLocations.B) + ")]"
                , testHarness.getOutput().toString());

        testHarness.close();
    }


    @NotNull
    private static KeyedOneInputStreamOperatorTestHarness<String, VehicleEvent, Tuple2<String, Trip>> createHarness(TripConstructorFunction fut) throws Exception {

        /* Cannot use the following as it implicitly opens the harness
                ProcessFunctionTestHarnesses
                .forKeyedProcessFunction(
                        fut,
                        vehicleEvent -> vehicleEvent.id,
                        Types.STRING
                 );
        This is particularly problematic when restoring state, as the initialization needs to happen prior to the opening
        */

        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(fut),
                (KeySelector<VehicleEvent, String>) ve -> ve.id ,
                Types.STRING,
                10, /* max parallelism */
                1 /* num subtasks */,
                0 /* subtask index */);
    }
}

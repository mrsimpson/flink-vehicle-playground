package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;

public class TripConstructorFunctionTest {
    @Test
    public void detectNewTrip() throws Exception {
        TripConstructorFunction fut = new TripConstructorFunction();
        KeyedOneInputStreamOperatorTestHarness<String, VehicleEvent, TripTuple> testHarness = createHarness(fut);
        testHarness.open();

        Date now = new Date();
        testHarness.processElement(new VehicleEvent("1", now, "provider_1", TestLocations.A, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP), 1);
        assertEquals("(1,Trip of 1 started " + new Tracking(now, TestLocations.A).toFormattedString() + " ongoing)"
                , ((StreamRecord<TripTuple>)testHarness.getOutput().toArray()[0]).getValue().toFormattedString());

        testHarness.close();
    }

    @Test
    public void endTrip() throws Exception {
        TripConstructorFunction fut = new TripConstructorFunction();

        // Mock an initial state in which a trip is ongoing
        KeyedOneInputStreamOperatorTestHarness<String, VehicleEvent, TripTuple> snapshotHarness = createHarness(fut);
        snapshotHarness.open();
        Date startDate = new Date();
        snapshotHarness.processElement(new VehicleEvent("1", startDate, "provider_1", TestLocations.A, VehicleEventType.TRIP_START, VehicleStateType.ON_TRIP), 1);
        OperatorSnapshotFinalizer snapshot = snapshotHarness.snapshotWithLocalState(1L, 1L);
        snapshotHarness.close();

        // Create a new harness as container for the actual test execution
        KeyedOneInputStreamOperatorTestHarness<String, VehicleEvent, TripTuple> testHarness = createHarness(fut);
        testHarness.initializeState(snapshot.getJobManagerOwnedState(), snapshot.getTaskLocalState());
        testHarness.open();

        // perform the actual test
        Date now = new Date();
        testHarness.processElement(new VehicleEvent("1", now, "provider_1", TestLocations.B, VehicleEventType.TRIP_END, VehicleStateType.AVAILABLE), 2);
        assertEquals("(1,Trip of 1 started " + new Tracking(startDate, TestLocations.A).toFormattedString() + " --> " + new Tracking(startDate, TestLocations.B).toFormattedString() + ")"
                , ((StreamRecord<TripTuple>)testHarness.getOutput().toArray()[0]).getValue().toFormattedString());

        testHarness.close();
    }

    @Test
    /*
      processing a trip end event without prior started event shall lead to an exception
      which is propagated to a side output
     */
    public void errorToSideOutput() throws Exception {
        TripConstructorFunction fut = new TripConstructorFunction();
        KeyedOneInputStreamOperatorTestHarness<String, VehicleEvent, TripTuple> testHarness = createHarness(fut);
        testHarness.open();

        testHarness.processElement(new VehicleEvent("1", new Date(), "provider_1", TestLocations.B, VehicleEventType.TRIP_END, VehicleStateType.AVAILABLE), 2);

        assertEquals(1, testHarness.getSideOutput(TripConstructorFunction.ERROR_OUTPUT_TAG).size());
    }


    @NotNull
    private static KeyedOneInputStreamOperatorTestHarness<String, VehicleEvent, TripTuple> createHarness(TripConstructorFunction fut) throws Exception {

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

package io.github.mrsimpson.vehicleStreaming.app;

import org.junit.Test;

import java.time.Duration;
import java.util.Date;

import static org.junit.Assert.*;

public class ParkingIntervalConstructorFunctionTest {

    @Test
    public void nextIntervalEndStartOfHour() {
        Date nextIntervalEnd = ParkingIntervalConstructorFunction.getNextIntervalEnd(new Date(0), Duration.ofMinutes(15));
        assertEquals(nextIntervalEnd, new Date(15 * 60_000));
    }

    @Test
    public void nextIntervalEndEndOfHour() {
        Date nextIntervalEnd = ParkingIntervalConstructorFunction.getNextIntervalEnd(new Date(59 * 60_000), Duration.ofMinutes(30));
        assertEquals(nextIntervalEnd, new Date(60 * 60_000));
    }

    @Test
    public void nextIntervalWithinHour() {
        Date nextIntervalEnd = ParkingIntervalConstructorFunction.getNextIntervalEnd(new Date(16 * 60_000), Duration.ofMinutes(15));
        assertEquals(nextIntervalEnd, new Date(2 * 15 * 60_000));
    }

    @Test
    public void twoHoursResolution() {
        Date nextIntervalEnd = ParkingIntervalConstructorFunction.getNextIntervalEnd(new Date(16 * 60_000), Duration.ofHours(2));
        assertEquals(nextIntervalEnd, new Date(2 * 60 * 60_000));
    }

    @Test
    public void notLongerThanADay() {
        try{
            ParkingIntervalConstructorFunction.getNextIntervalEnd(new Date(), Duration.ofHours(25));
            fail(); // must not get here
        } catch (Error e) {
            assertNotNull(e);
        }
    }

}

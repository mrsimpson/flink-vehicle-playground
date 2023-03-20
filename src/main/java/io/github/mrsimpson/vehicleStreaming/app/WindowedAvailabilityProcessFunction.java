package io.github.mrsimpson.vehicleStreaming.app;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class WindowedAvailabilityProcessFunction extends ProcessWindowFunction<Tuple2<String, Long>, ProviderWindowedAvailability, String, TimeWindow> {
    /**
     * @param provider:                       The provider acts as key
     * @param context:                        The context in which the window is being evaluated.
     * @param aggregatedAvailabilityIterator: The list of parking intervals ot this key within the window
     * @param out:                            A collector for emitting elements.
     */
    @Override
    public void process(String provider,
                        ProcessWindowFunction<Tuple2<String, Long>, ProviderWindowedAvailability, String, TimeWindow>.Context context,
                        Iterable<Tuple2<String, Long>> aggregatedAvailabilityIterator,
                        Collector<ProviderWindowedAvailability> out) {
        // there will be only one element, we're just appending information from the window
        Tuple2<String, Long> aggregatedAvailability = aggregatedAvailabilityIterator.iterator().next();

        long start = context.window().getStart();
        long end = context.window().getEnd();

        out.collect(
                new ProviderWindowedAvailability(
                        aggregatedAvailability.f0,
                        aggregatedAvailability.f1,
                        aggregatedAvailability.f1.doubleValue() / (end - start),
                        Instant.ofEpochMilli(start),
                        Instant.ofEpochMilli(end)
        ));
    }
}


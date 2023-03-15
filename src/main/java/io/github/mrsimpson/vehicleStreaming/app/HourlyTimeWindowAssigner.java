package io.github.mrsimpson.vehicleStreaming.app;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;

class HourlyTimeWindowAssigner<T> extends WindowAssigner<T, TimeWindow> {

    private final long windowSize = 60 * 60 * 1000L;

    @Override
    public Collection<TimeWindow> assignWindows(T parkingIntervalTuple, long timestamp, WindowAssignerContext windowAssignerContext) {
        long startTime = timestamp - (timestamp % windowSize);
        long endTime = startTime + windowSize;
        return Collections.<TimeWindow>singletonList(new TimeWindow(startTime, endTime));
    }

    @Override
    public Trigger<T, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
        return (Trigger) EventTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return true;
    }
}

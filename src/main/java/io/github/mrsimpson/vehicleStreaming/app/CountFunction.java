package io.github.mrsimpson.vehicleStreaming.app;

import io.github.mrsimpson.vehicleStreaming.util.VehicleEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CountFunction extends KeyedProcessFunction<String, VehicleEvent, Tuple2<String, Integer>> {
    private transient ValueState<Integer> count;

    @Override
    public void processElement(VehicleEvent value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        int newCount = count.value() == null ? 1 : count.value() + 1;
        count.update(newCount);
        out.collect(new Tuple2<>(ctx.getCurrentKey(), newCount));
    }

    /*
    @see https://amareswar-polimera.medium.com/basic-stateful-word-count-using-apache-flink-c68fae97f2ba
    The process function becomes stateful because of the implementation we have in the open method.
    Any state can be retrieved from the flink’s runtime context using a descriptor.
    Here we are using a ValueDescriptor that has a name as count and the value is of type Integer
    */
    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("count", Integer.class);
        count = getRuntimeContext().getState(valueStateDescriptor);
    }
}

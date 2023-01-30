package io.github.mrsimpson.vehicleStreaming.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.List;

public class Tuple2TestSinkFunction extends RichSinkFunction<Tuple2<String, Integer>> {
    private final List<Tuple2<String, Integer>> collectedElements = new ArrayList<>();
    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) {
        collectedElements.add(value);
    }

    public List<Tuple2<String, Integer>> getCollectedElements() {
        return collectedElements;
    }
}

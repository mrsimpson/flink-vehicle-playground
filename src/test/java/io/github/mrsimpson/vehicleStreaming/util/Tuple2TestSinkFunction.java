package io.github.mrsimpson.vehicleStreaming.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class Tuple2TestSinkFunction extends RichSinkFunction<Tuple2<String, Integer>> {

    // Since the Sink set up during th graph construction is another instance than the one used when running the job,
    // the buffer during the test needs to be static
    // @see https://anton-bakalets.medium.com/flink-job-unit-testing-df4f618d07a6
    // @see https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/testing/#testing-flink-jobs
    private static final List<Tuple2<String, Integer>> collectedElements = new CopyOnWriteArrayList<>();
    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) {
        collectedElements.add(value);
    }

    public List<Tuple2<String, Integer>> getCollectedElements() {
        return collectedElements;
    }

    public void clear(){
        collectedElements.clear();
    }
}

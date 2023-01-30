package io.github.mrsimpson.vehicleStreaming.util;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class NullSink<T> extends RichSinkFunction<T> {
    @Override
    public void invoke(Object value, Context context){
        // do nothing
    }
}

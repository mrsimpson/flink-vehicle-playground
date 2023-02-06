package io.github.mrsimpson.vehicleStreaming.util;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

public class NullSink<T> implements Sink<T>{

    @Override
    public SinkWriter<T> createWriter(InitContext initContext) {
        return new SinkWriter<>(){

            @Override
            public void write(Object o, Context context) {
                // do nothing
            }
            @Override
            public void close() {
                // not here
            }

            @Override
            public void flush(boolean b)  {
                // not there
            }
        };
    }
}

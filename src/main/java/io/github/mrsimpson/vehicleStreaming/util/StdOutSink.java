package io.github.mrsimpson.vehicleStreaming.util;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

/**
 * A simple Sink writing to standard out.
 * Similar to the PrintSinkFunction, but instead of being a SinkFunction, this is a Sink
 */
public class StdOutSink<IN> implements Sink<IN> {

    private final String identifier;

    public StdOutSink(String identifier) {
        this.identifier = identifier;
    }

    @Override
    public SinkWriter<IN> createWriter(InitContext initContext) {
        return new SinkWriter<>() {
            @Override
            public void write(Object o, Context context) {
                String text = (o instanceof FormattablePojo)
                        ? ((FormattablePojo) o).toFormattedString()
                        : o.toString();
                System.out.println(identifier + " => " + text);
            }

            @Override
            public void flush(boolean b) {

            }

            @Override
            public void close() {

            }
        };
    }
}
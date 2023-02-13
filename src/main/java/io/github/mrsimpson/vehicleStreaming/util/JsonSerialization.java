package io.github.mrsimpson.vehicleStreaming.util;

import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

public class JsonSerialization {
    public static <IN> JsonSerializationSchema<IN> getJsonSerializationSchema() {
        return new JsonSerializationSchema<>(){
            @Override
            public byte[] serialize(IN element) {
                try {
                    return this.mapper.writeValueAsBytes(element);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}

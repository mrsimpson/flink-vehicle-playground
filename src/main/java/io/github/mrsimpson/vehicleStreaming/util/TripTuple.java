package io.github.mrsimpson.vehicleStreaming.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.StringUtils;

public class TripTuple extends Tuple2<String, Trip> implements FormattablePojo{

    // Parameterless constructor needed for Flink serialization
    public TripTuple(){}

    public TripTuple(String id, Trip trip){
        super(id, trip);
    }
    @Override
    public String toFormattedString() {
        return "(" + StringUtils.arrayAwareToString(this.f0) + "," + StringUtils.arrayAwareToString(this.f1.toFormattedString()) + ")";
    }
}

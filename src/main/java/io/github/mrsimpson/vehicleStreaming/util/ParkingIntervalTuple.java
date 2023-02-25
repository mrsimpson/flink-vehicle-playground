package io.github.mrsimpson.vehicleStreaming.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.StringUtils;

public class ParkingIntervalTuple extends Tuple2<String, ParkingInterval> implements FormattablePojo{

    // Parameterless constructor needed for Flink serialization
    public ParkingIntervalTuple(){}

    public ParkingIntervalTuple(String id, ParkingInterval parkingInterval){
        super(id, parkingInterval);
    }
    @Override
    public String toFormattedString() {
        return "(" + StringUtils.arrayAwareToString(this.f0) + "," + StringUtils.arrayAwareToString(this.f1.toFormattedString()) + ")";
    }
}

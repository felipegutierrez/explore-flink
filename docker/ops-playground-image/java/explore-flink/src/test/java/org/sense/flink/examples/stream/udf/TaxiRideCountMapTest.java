package org.sense.flink.examples.stream.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TaxiRideCountMapTest {

    @Test
    public void map() throws Exception {
        // instantiate your function
        TaxiRideCountMap mapFunction = new TaxiRideCountMap();

        Tuple2<Boolean, Tuple2<Long, Long>> value = Tuple2.of(true, Tuple2.of(1L, 2L));

        // call the methods that you have implemented
        assertEquals("true|1-2", mapFunction.map(value));
    }
}
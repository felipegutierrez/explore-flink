package org.sense.flink.examples.stream.tpch.udf;

import org.apache.flink.api.java.tuple.Tuple11;
import org.junit.Test;
import org.sense.flink.examples.stream.tpch.pojo.LineItem;

import static org.junit.Assert.assertEquals;

public class LineItemToTuple11MapTest {

    @Test
    public void map() throws Exception {
        // instantiate your function
        LineItemToTuple11Map mapFunction = new LineItemToTuple11Map();

        LineItem lineItem = new LineItem(1L, 2L, 3L, 4L, 5, 6L,
                7L, 8L, 9L, "flag", "status", 123,
                10, 11, "shipInstructions", "shipMode", "comment");
        Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> expected =
                Tuple11.of("flag", "status", 6L, 0.07, 0.08, 0.06440000000000001, 0.07019600000000002, 6L, 0.07, 0.08, 1L);

        // call the methods that you have implemented
        assertEquals(expected, mapFunction.map(lineItem));
    }
}
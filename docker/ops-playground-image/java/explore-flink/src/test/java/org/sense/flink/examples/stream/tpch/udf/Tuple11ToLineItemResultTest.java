package org.sense.flink.examples.stream.tpch.udf;

import org.apache.flink.api.java.tuple.Tuple11;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Tuple11ToLineItemResultTest {

    @Test
    public void map() throws Exception {
        // instantiate your function
        Tuple11ToLineItemResult mapFunction = new Tuple11ToLineItemResult();

        Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> value =
                Tuple11.of("A", "B", 0L, 1.1, 2.2, 3.3, 4.4, 5L, 6.6, 7.7, 8L);
        String expected = "flag:A status:B sum_qty:0 sum_base_price:1.1 sum_disc:2.2 sum_disc_price:3.3 sum_charge:"
                + "4.4 avg_qty:5 avg_price:6.6 avg_disc:7.7 order_qty:8";

        // call the methods that you have implemented
        assertEquals(expected, mapFunction.map(value));
    }
}
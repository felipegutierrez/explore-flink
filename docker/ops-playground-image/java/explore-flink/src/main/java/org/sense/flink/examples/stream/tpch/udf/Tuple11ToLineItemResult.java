package org.sense.flink.examples.stream.tpch.udf;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple11;

/**
 * <pre>
 *        l_returnflag, // string
 *        l_linestatus, // string
 *        sum(l_quantity) as sum_qty, // Long
 *        sum(l_extendedprice) as sum_base_price, // Double
 *        sum(l_discount) as sum_disc // Double
 *        sum(l_extendedprice * (1-l_discount)) as sum_disc_price, // Double
 *        sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge, // Double
 *        avg(l_quantity) as avg_qty, // long
 *        avg(l_extendedprice) as avg_price, // Double
 *        avg(l_discount) as avg_disc, // Double
 *        count(*) as count_order // long
 * </pre>
 */
public class Tuple11ToLineItemResult implements MapFunction<Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>, String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String map(Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> value) throws Exception {
        String msg = "flag:" + value.f0 +
                " status:" + value.f1 +
                " sum_qty:" + value.f2 +
                " sum_base_price:" + value.f3 +
                " sum_disc:" + value.f4 +
                " sum_disc_price:" + value.f5 +
                " sum_charge:" + value.f6 +
                " avg_qty:" + value.f7 +
                " avg_price:" + value.f8 +
                " avg_disc:" + value.f9 +
                " order_qty:" + value.f10;
        return msg;
    }
}

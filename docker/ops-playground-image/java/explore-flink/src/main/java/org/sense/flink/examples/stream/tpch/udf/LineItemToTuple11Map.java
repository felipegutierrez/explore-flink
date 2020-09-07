package org.sense.flink.examples.stream.tpch.udf;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.sense.flink.examples.stream.tpch.pojo.LineItem;

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
public class LineItemToTuple11Map implements MapFunction<LineItem, Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> {
    private static final long serialVersionUID = 1L;

    @Override
    public Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> map(LineItem lineItem) throws Exception {

        return Tuple11.of(lineItem.getReturnFlag(), lineItem.getStatus(),
                lineItem.getQuantity(),
                lineItem.getExtendedPrice(),
                lineItem.getDiscount(),
                lineItem.getExtendedPrice() * (1 - lineItem.getDiscount()),
                lineItem.getExtendedPrice() * (1 - lineItem.getDiscount()) * (1 + lineItem.getTax()),
                lineItem.getQuantity(),
                lineItem.getExtendedPrice(),
                lineItem.getDiscount(),
                1L);
    }
}

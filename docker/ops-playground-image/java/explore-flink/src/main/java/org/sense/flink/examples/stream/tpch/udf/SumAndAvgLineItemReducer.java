package org.sense.flink.examples.stream.tpch.udf;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple11;

public class SumAndAvgLineItemReducer implements ReduceFunction<Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> {
    private static final long serialVersionUID = 1L;

    @Override
    public Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> reduce(
            Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> value1,
            Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> value2) throws Exception {
        Long count = value1.f10 + value2.f10;
        Long sumQty = value1.f2 + value2.f2;
        Double sumBasePrice = value1.f3 + value2.f3;
        Double sumDisc = value1.f4 + value2.f4;
        Double sumDiscPrice = value1.f5 + value2.f5;
        Double sumCharge = value1.f6 + value2.f6;
        Long avgQty = sumQty / count;
        Double avgBasePrice = sumBasePrice / count;
        Double avgDisc = sumDisc / count;

        return Tuple11.of(
                value1.f0,
                value1.f1,
                sumQty,
                sumBasePrice,
                sumDisc,
                sumDiscPrice,
                sumCharge,
                avgQty,
                avgBasePrice,
                avgDisc,
                count);
    }
}

package org.sense.flink.examples.stream.tpch.udf;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple2;

public class LineItemFlagAndStatusKeySelector implements KeySelector<Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>, Tuple2<String, String>> {
    private static final long serialVersionUID = 1L;

    @Override
    public Tuple2<String, String> getKey(Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long> lineItem) throws Exception {
        return Tuple2.of(lineItem.f0, lineItem.f1);
    }
}

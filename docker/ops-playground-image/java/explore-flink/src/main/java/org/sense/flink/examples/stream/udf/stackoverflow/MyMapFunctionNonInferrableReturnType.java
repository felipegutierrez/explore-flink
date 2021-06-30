package org.sense.flink.examples.stream.udf.stackoverflow;

import org.apache.flink.api.common.functions.MapFunction;

public class MyMapFunctionNonInferrableReturnType<T> implements MapFunction<AbstractDataModel, T> {

    @Override
    public T map(AbstractDataModel value) throws Exception {
        return (T) value.getValue();
    }
}

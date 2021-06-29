package org.sense.flink.examples.stream.udf.stackoverflow;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

public class MyMapFunction implements MapFunction<AbstractDataModel, String>, ResultTypeQueryable {

    @Override
    public String map(AbstractDataModel value) throws Exception {
        return value.getValue();
    }

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(AbstractDataModel.class);
    }
}

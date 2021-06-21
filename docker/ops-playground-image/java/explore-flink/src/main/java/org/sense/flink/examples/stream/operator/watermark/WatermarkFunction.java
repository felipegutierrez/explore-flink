package org.sense.flink.examples.stream.operator.watermark;

import org.apache.flink.api.common.functions.Function;
import java.io.Serializable;

public interface WatermarkFunction<T> extends Function, Serializable {

    T process(T value) throws Exception;
}

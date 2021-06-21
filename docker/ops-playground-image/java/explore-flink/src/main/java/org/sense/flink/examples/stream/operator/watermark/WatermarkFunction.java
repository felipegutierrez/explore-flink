package org.sense.flink.examples.stream.operator.watermark;

import org.apache.flink.api.common.functions.Function;
import java.io.Serializable;

public interface WatermarkFunction<T> extends Function, Serializable {

    boolean process(T value) throws Exception;
}

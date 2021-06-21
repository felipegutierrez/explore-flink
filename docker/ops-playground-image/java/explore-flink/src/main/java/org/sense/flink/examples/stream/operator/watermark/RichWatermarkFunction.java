package org.sense.flink.examples.stream.operator.watermark;

import org.apache.flink.api.common.functions.AbstractRichFunction;

public abstract class RichWatermarkFunction<T> extends AbstractRichFunction
        implements WatermarkFunction<T> {
    private static final long serialVersionUID = 1L;

    @Override
    public abstract T process(T value) throws Exception;
}

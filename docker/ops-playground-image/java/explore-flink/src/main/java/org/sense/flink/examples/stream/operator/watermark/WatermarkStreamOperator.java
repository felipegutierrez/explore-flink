package org.sense.flink.examples.stream.operator.watermark;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;

public class WatermarkStreamOperator<IN> extends AbstractUdfStreamOperator<IN, WatermarkFunction<IN>>
        implements OneInputStreamOperator<IN, IN> {

    private static final long serialVersionUID = 1L;

    private ListState<Long> latestWatermark;

    public WatermarkStreamOperator(WatermarkFunction<IN> mapper) {
        super(mapper);
        chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>("latest-watermark", Long.class);
        this.latestWatermark = context.getOperatorStateStore().getListState(descriptor);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        output.collect(element);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if (mark.isWatermark()) {
            List<Long> watermarkListState = new ArrayList<>();
            latestWatermark.get().forEach(watermarkListState::add);
            watermarkListState.add(mark.getTimestamp());
            latestWatermark.update(watermarkListState);
        }
        super.processWatermark(mark);
    }
}

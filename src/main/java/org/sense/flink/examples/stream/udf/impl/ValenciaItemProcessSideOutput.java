package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.util.ValenciaItemType;

public class ValenciaItemProcessSideOutput extends ProcessFunction<ValenciaItem, ValenciaItem> {
	private static final long serialVersionUID = -8186522079661070009L;
	private OutputTag<Tuple2<ValenciaItemType, Long>> outputTag;

	public ValenciaItemProcessSideOutput(OutputTag<Tuple2<ValenciaItemType, Long>> outputTag) {
		this.outputTag = outputTag;
	}

	@Override
	public void processElement(ValenciaItem valenciaItem, ProcessFunction<ValenciaItem, ValenciaItem>.Context ctx,
			Collector<ValenciaItem> out) throws Exception {
		out.collect(valenciaItem);
		ctx.output(outputTag, Tuple2.of(valenciaItem.getType(), valenciaItem.getId()));
	}
}

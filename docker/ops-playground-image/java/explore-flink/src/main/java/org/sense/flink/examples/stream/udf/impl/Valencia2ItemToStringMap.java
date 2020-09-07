package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.sense.flink.pojo.ValenciaItem;

public class Valencia2ItemToStringMap extends RichMapFunction<Tuple2<ValenciaItem, ValenciaItem>, String> {
	private static final long serialVersionUID = 3023076498228433840L;

	@Override
	public String map(Tuple2<ValenciaItem, ValenciaItem> value) throws Exception {
		return value.f0 + " - " + value.f1;
	}
}

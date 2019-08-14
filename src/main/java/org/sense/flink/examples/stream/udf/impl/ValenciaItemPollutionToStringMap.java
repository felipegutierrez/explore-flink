package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.pojo.ValenciaPollution;

public class ValenciaItemPollutionToStringMap extends RichMapFunction<Tuple2<ValenciaItem, ValenciaPollution>, String> {
	private static final long serialVersionUID = 1823600535737193529L;

	@Override
	public String map(Tuple2<ValenciaItem, ValenciaPollution> value) throws Exception {
		return value.f0 + " - " + value.f1;
	}
}

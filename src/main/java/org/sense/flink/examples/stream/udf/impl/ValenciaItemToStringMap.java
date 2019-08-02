package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.sense.flink.pojo.ValenciaItem;

public class ValenciaItemToStringMap extends RichMapFunction<Tuple2<Long, ValenciaItem>, String> {
	private static final long serialVersionUID = -2432530358879710931L;

	@Override
	public String map(Tuple2<Long, ValenciaItem> value) throws Exception {
		return value.f1.getType() + "         qtd[" + value.f0 + "] " + value.f1.toString();
	}
}

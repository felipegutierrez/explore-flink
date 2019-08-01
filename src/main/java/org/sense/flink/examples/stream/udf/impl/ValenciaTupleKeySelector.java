package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.sense.flink.pojo.ValenciaItem;

public class ValenciaTupleKeySelector implements KeySelector<Tuple2<Long, ValenciaItem>, Long> {
	private static final long serialVersionUID = 3749555561769812058L;

	@Override
	public Long getKey(Tuple2<Long, ValenciaItem> value) throws Exception {
		return value.f0;
	}
}

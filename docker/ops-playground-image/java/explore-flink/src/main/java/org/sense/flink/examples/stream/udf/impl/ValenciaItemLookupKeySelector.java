package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.sense.flink.util.ValenciaItemType;

public class ValenciaItemLookupKeySelector implements KeySelector<Tuple2<ValenciaItemType, Long>, Long> {
	private static final long serialVersionUID = -3512727974712750956L;

	@Override
	public Long getKey(Tuple2<ValenciaItemType, Long> value) throws Exception {
		return value.f1;
	}
}

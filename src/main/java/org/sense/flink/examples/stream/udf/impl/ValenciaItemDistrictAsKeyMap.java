package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.sense.flink.pojo.ValenciaItem;

public class ValenciaItemDistrictAsKeyMap extends RichMapFunction<ValenciaItem, Tuple2<Long, ValenciaItem>> {
	private static final long serialVersionUID = -7279379880447276103L;

	@Override
	public Tuple2<Long, ValenciaItem> map(ValenciaItem value) throws Exception {
		return Tuple2.of(value.getId(), value);
	}
}

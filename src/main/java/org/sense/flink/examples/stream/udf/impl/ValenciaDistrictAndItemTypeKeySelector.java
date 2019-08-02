package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.util.ValenciaItemType;

public class ValenciaDistrictAndItemTypeKeySelector
		implements KeySelector<Tuple2<Long, ValenciaItem>, Tuple2<Long, ValenciaItemType>> {
	private static final long serialVersionUID = 3749555561769812058L;

	@Override
	public Tuple2<Long, ValenciaItemType> getKey(Tuple2<Long, ValenciaItem> value) throws Exception {
		return Tuple2.of(value.f0, value.f1.getType());
	}
}

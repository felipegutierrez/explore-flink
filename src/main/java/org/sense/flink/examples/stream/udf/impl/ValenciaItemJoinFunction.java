package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.sense.flink.pojo.ValenciaItem;

public class ValenciaItemJoinFunction
		implements JoinFunction<ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>> {
	private static final long serialVersionUID = -5624248427888414054L;

	@Override
	public Tuple2<ValenciaItem, ValenciaItem> join(ValenciaItem first, ValenciaItem second) throws Exception {
		return Tuple2.of(first, second);
	}
}

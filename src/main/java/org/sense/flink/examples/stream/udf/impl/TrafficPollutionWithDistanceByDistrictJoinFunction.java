package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.sense.flink.pojo.ValenciaItem;

public class TrafficPollutionWithDistanceByDistrictJoinFunction
		extends RichJoinFunction<ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>> {
	private static final long serialVersionUID = -8504552660558428217L;

	@Override
	public Tuple2<ValenciaItem, ValenciaItem> join(ValenciaItem traffic, ValenciaItem pollution) throws Exception {

		return Tuple2.of(traffic, pollution);
	}
}

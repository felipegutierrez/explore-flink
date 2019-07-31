package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.sense.flink.pojo.ValenciaTraffic;

public class ValenciaTrafficAssignDistrictAsKeyMap
		extends RichMapFunction<ValenciaTraffic, Tuple2<Long, ValenciaTraffic>> {
	private static final long serialVersionUID = 1271441725018914893L;

	@Override
	public Tuple2<Long, ValenciaTraffic> map(ValenciaTraffic value) throws Exception {
		return Tuple2.of(value.getId(), value);
	}
}

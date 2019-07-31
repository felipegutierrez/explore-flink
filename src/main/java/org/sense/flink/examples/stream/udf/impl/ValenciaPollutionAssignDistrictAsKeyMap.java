package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.sense.flink.pojo.ValenciaPollution;

public class ValenciaPollutionAssignDistrictAsKeyMap
		extends RichMapFunction<ValenciaPollution, Tuple2<Long, ValenciaPollution>> {
	private static final long serialVersionUID = -2931982637024625353L;

	@Override
	public Tuple2<Long, ValenciaPollution> map(ValenciaPollution value) throws Exception {
		return Tuple2.of(value.getId(), value);
	}
}

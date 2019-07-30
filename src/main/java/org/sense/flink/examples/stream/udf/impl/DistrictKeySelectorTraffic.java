package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.java.functions.KeySelector;
import org.sense.flink.pojo.ValenciaTraffic;

public class DistrictKeySelectorTraffic implements KeySelector<ValenciaTraffic, Long> {
	private static final long serialVersionUID = 5552661239654626468L;

	@Override
	public Long getKey(ValenciaTraffic value) throws Exception {
		return value.getId();
	}
}

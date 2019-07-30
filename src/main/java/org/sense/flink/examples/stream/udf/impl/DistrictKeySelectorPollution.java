package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.java.functions.KeySelector;
import org.sense.flink.pojo.ValenciaPollution;

public class DistrictKeySelectorPollution implements KeySelector<ValenciaPollution, Long> {
	private static final long serialVersionUID = -476605948742022387L;

	@Override
	public Long getKey(ValenciaPollution value) throws Exception {
		return value.getId();
	}
}

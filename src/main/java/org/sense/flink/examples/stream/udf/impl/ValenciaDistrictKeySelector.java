package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.java.functions.KeySelector;
import org.sense.flink.pojo.ValenciaItem;

public class ValenciaDistrictKeySelector implements KeySelector<ValenciaItem, Long> {
	private static final long serialVersionUID = 8670966631240512086L;

	@Override
	public Long getKey(ValenciaItem value) throws Exception {
		return value.getId();
	}
}

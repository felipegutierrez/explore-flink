package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.sense.flink.pojo.ValenciaItem;

public class ValenciaItemAscendingTimestampExtractor extends AscendingTimestampExtractor<ValenciaItem> {
	private static final long serialVersionUID = 4311406052896755965L;

	@Override
	public long extractAscendingTimestamp(ValenciaItem valenciaItem) {
		return valenciaItem.getTimestamp();
	}
}

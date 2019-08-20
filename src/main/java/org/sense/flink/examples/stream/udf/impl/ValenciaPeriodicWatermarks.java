package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.sense.flink.pojo.ValenciaItem;

public class ValenciaPeriodicWatermarks implements AssignerWithPeriodicWatermarks<ValenciaItem> {
	private static final long serialVersionUID = 6344386038569216902L;

	private final long maxTimeLag = 5000; // 5 seconds

	@Override
	public long extractTimestamp(ValenciaItem element, long previousElementTimestamp) {
		return element.getTimestamp();
	}

	@Override
	public Watermark getCurrentWatermark() {
		// return the watermark as current time minus the maximum time lag
		return new Watermark(System.currentTimeMillis() - maxTimeLag);
	}
}

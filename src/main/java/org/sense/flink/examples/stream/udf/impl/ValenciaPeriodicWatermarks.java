package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.sense.flink.pojo.ValenciaItem;

public class ValenciaPeriodicWatermarks implements AssignerWithPeriodicWatermarks<ValenciaItem> {
	private static final long serialVersionUID = 6344386038569216902L;
	private long windowTime;

	public ValenciaPeriodicWatermarks(long windowTime) throws Exception {
		if (windowTime > 0) {
			this.windowTime = windowTime;
		} else {
			throw new Exception("The window time must be greater the 0!");
		}
	}

	@Override
	public long extractTimestamp(ValenciaItem element, long previousElementTimestamp) {
		return element.getTimestamp();
	}

	/**
	 * Set the watermarker @windowTime seconds behind the current time. Make sure to
	 * use
	 * env.getConfig().setAutoWatermarkInterval(Time.seconds(60).toMilliseconds());
	 * to emit watermarkers only each 60 seconds as well.
	 */
	@Override
	public Watermark getCurrentWatermark() {
		// the watermark is 60 seconds behind of the current time
		return new Watermark(System.currentTimeMillis() - windowTime);
	}
}

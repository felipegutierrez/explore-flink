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
	 * 
	 * getCurrentWatermark() should NOT be implemented in terms of
	 * System.currentTimeMillis -- you do not want your watermarking to depend on
	 * the current processing time if you can possibly avoid it. Part of the beauty
	 * of event time processing is being able to run your application on historic
	 * data as well as live, real-time data, and this is only possible if your
	 * watermarks depend on timestamps recorded in the events, rather than
	 * System.currentTimeMillis.
	 */
	@Override
	public Watermark getCurrentWatermark() {
		// the watermark is 60 seconds behind of the current time
		return new Watermark(System.currentTimeMillis() - windowTime);
	}
}

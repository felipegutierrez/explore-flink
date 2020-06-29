package org.sense.flink.examples.stream.tpch.udf;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.sense.flink.examples.stream.tpch.pojo.Order;

public class OrderTimestampAndWatermarkAssigner extends AscendingTimestampExtractor<Order> {
	private static final long serialVersionUID = 1L;

	@Override
	public long extractAscendingTimestamp(Order element) {
		return element.getTimestamp();
	}
}

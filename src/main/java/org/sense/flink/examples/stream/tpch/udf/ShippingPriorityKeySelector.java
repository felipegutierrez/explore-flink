package org.sense.flink.examples.stream.tpch.udf;

import org.apache.flink.api.java.functions.KeySelector;
import org.sense.flink.examples.stream.tpch.pojo.ShippingPriorityItem;

public class ShippingPriorityKeySelector implements KeySelector<ShippingPriorityItem, Long> {
	private static final long serialVersionUID = 1L;

	@Override
	public Long getKey(ShippingPriorityItem value) throws Exception {
		return value.getOrderkey();
	}
}

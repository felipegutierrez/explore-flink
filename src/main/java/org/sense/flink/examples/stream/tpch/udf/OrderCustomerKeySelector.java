package org.sense.flink.examples.stream.tpch.udf;

import org.apache.flink.api.java.functions.KeySelector;
import org.sense.flink.examples.stream.tpch.pojo.Order;

public class OrderCustomerKeySelector implements KeySelector<Order, Long> {
	private static final long serialVersionUID = 1L;

	@Override
	public Long getKey(Order value) throws Exception {
		return value.getCustomerKey();
	}
}

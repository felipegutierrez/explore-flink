package org.sense.flink.examples.stream.tpch.udf;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.sense.flink.examples.stream.tpch.pojo.ShippingPriorityItem;

public class ShippingPriority3KeySelector implements KeySelector<ShippingPriorityItem, Tuple3<Long, String, Integer>> {
	private static final long serialVersionUID = 1L;

	@Override
	public Tuple3<Long, String, Integer> getKey(ShippingPriorityItem value) throws Exception {
		return Tuple3.of(value.getOrderkey(), value.getOrderdate(), value.getShippriority());
	}
}

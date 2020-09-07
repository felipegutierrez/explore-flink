package org.sense.flink.examples.stream.tpch.udf;

import org.apache.flink.api.common.functions.FilterFunction;
import org.sense.flink.examples.stream.tpch.pojo.Customer;

public class CustomerFilter implements FilterFunction<Customer> {
	private static final long serialVersionUID = 1L;

	private String marketSegment;

	public CustomerFilter() {
		this("AUTOMOBILE");
	}

	public CustomerFilter(String marketSegment) {
		this.marketSegment = marketSegment;
	}

	@Override
	public boolean filter(Customer c) {
		return c.getMarketSegment().equals(marketSegment);
	}
}

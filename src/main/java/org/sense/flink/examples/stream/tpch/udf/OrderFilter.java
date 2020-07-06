package org.sense.flink.examples.stream.tpch.udf;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.common.functions.FilterFunction;
import org.sense.flink.examples.stream.tpch.pojo.Order;

public class OrderFilter implements FilterFunction<Order> {
	private static final long serialVersionUID = 1L;
	private final DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private final Date date;

	public OrderFilter() throws ParseException {
		date = sdf.parse("1995-03-12");
	}

	@Override
	public boolean filter(Order o) throws ParseException {
		Date orderDate = new Date(o.getOrderDate());
		return orderDate.before(date);
	}
}

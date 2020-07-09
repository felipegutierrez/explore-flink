package org.sense.flink.examples.stream.tpch.udf;

import org.apache.flink.api.common.functions.FilterFunction;
import org.sense.flink.examples.stream.tpch.pojo.Order;

import java.text.ParseException;

public class OrderYearFilter implements FilterFunction<Order> {
    private static final long serialVersionUID = 1L;
    private final int year;

    public OrderYearFilter() throws ParseException {
        this.year = 1990;
    }

    public OrderYearFilter(int year) throws ParseException {
        this.year = year;
    }

    @Override
    public boolean filter(Order o) throws ParseException {
        int value = OrdersSource.getYear(o.getOrderDate());
        return value > year;
    }
}

package org.sense.flink.examples.stream.tpch.udf;

import org.apache.flink.api.common.functions.FilterFunction;
import org.sense.flink.examples.stream.tpch.pojo.Order;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OrderDateFilter implements FilterFunction<Order> {
    private static final long serialVersionUID = 1L;

    private final DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private final Date date;

    public OrderDateFilter() throws ParseException {
        date = sdf.parse("1995-03-12");
    }

    public OrderDateFilter(String yyyymmdd) throws ParseException {
        date = sdf.parse(yyyymmdd);
    }

    // int year = OrdersSource.getYear(o.getOrderDate());
    @Override
    public boolean filter(Order o) throws ParseException {
        String dateValue = OrdersSource.format(o.getOrderDate());
        return sdf.parse(dateValue).before(date);
    }
}

package org.sense.flink.examples.stream.tpch.udf;

import org.junit.Test;
import org.sense.flink.examples.stream.tpch.pojo.Order;

import java.text.ParseException;

import static org.junit.Assert.assertEquals;

public class OrderDateFilterTest {

    @Test
    public void filterFalse() throws ParseException {
        // instantiate your function
        OrderDateFilter filterFunction = new OrderDateFilter("2020-05-20");

        long orderKey = 0;
        long rowNumber = 1;
        long customerKey = 2;
        char orderStatus = 'a';
        long totalPrice = 3;
        int orderDate = 20201025;
        String orderPriority = "B";
        String clerk = "C";
        int shipPriority = 5;
        String comment = "C";
        Order order = new Order(rowNumber, orderKey, customerKey, orderStatus, totalPrice, orderDate, orderPriority,
                clerk, shipPriority, comment);

        assertEquals(false, filterFunction.filter(order));
    }

    @Test
    public void filterTrue() throws ParseException {
        // instantiate your function
        OrderDateFilter filterFunction = new OrderDateFilter("2020-05-20");

        long orderKey = 0;
        long rowNumber = 1;
        long customerKey = 2;
        char orderStatus = 'a';
        long totalPrice = 3;
        int orderDate = 20191025;
        String orderPriority = "B";
        String clerk = "C";
        int shipPriority = 5;
        String comment = "C";
        Order order = new Order(rowNumber, orderKey, customerKey, orderStatus, totalPrice, orderDate, orderPriority,
                clerk, shipPriority, comment);

        assertEquals(true, filterFunction.filter(order));
    }
}
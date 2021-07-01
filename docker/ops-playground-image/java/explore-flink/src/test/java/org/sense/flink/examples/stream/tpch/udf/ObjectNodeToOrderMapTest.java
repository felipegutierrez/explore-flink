package org.sense.flink.examples.stream.tpch.udf;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;
import org.sense.flink.examples.stream.tpch.pojo.Order;

import static org.junit.Assert.assertEquals;

public class ObjectNodeToOrderMapTest {

    @Test
    public void map() throws Exception {
        // instantiate your function
        ObjectNodeToOrderMap mapFunction = new ObjectNodeToOrderMap();

        long orderKey = 0;
        long rowNumber = 1;
        long customerKey = 2;
        char orderStatus = 'a';
        long totalPrice = 3;
        int orderDate = 4;
        String orderPriority = "B";
        String clerk = "C";
        int shipPriority = 5;
        String comment = "C";
        Order order = new Order(rowNumber, orderKey, customerKey, orderStatus, totalPrice, orderDate, orderPriority,
                clerk, shipPriority, comment);

        ObjectMapper mapper = new ObjectMapper();

        ObjectNode value = mapper.createObjectNode();
        value.put("rowNumber", 1);
        value.put("customerKey", 2);
        value.put("orderStatus", 'a');
        value.put("totalPrice", 3);
        value.put("orderDate", 4);
        value.put("orderPriority", "B");
        value.put("clerk", "C");
        value.put("shipPriority", 5);
        value.put("comment", "C");

        ObjectNode key = mapper.createObjectNode();
        key.put("key", 0);
        key.put("value", value);

        assertEquals(order.getValue(), mapFunction.map(key).getValue());
    }

    @Test(expected = NullPointerException.class)
    public void mapException() throws Exception {
        // instantiate your function
        ObjectNodeToOrderMap mapFunction = new ObjectNodeToOrderMap();

        long orderKey = 0;
        long rowNumber = 1;
        long customerKey = 2;
        char orderStatus = 'a';
        long totalPrice = 3;
        int orderDate = 4;
        String orderPriority = "B";
        String clerk = "C";
        int shipPriority = 5;
        String comment = "C";
        Order order = new Order(rowNumber, orderKey, customerKey, orderStatus, totalPrice, orderDate, orderPriority,
                clerk, shipPriority, comment);

        ObjectMapper mapper = new ObjectMapper();

        ObjectNode value = mapper.createObjectNode();
        value.put("rowNumber", 1);
        value.put("customerKey", 2);
        value.put("orderStatus", 'a');
        value.put("totalPrice", 3);

        ObjectNode key = mapper.createObjectNode();
        key.put("key", 0);
        key.put("value", value);

        assertEquals(order.getValue(), mapFunction.map(key).getValue());
    }
}

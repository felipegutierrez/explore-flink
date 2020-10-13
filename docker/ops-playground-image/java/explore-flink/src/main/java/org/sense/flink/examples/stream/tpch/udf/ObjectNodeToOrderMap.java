package org.sense.flink.examples.stream.tpch.udf;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.sense.flink.examples.stream.tpch.pojo.Order;

public class ObjectNodeToOrderMap implements MapFunction<ObjectNode, Order> {
    private static final long serialVersionUID = 1L;

    @Override
    public Order map(ObjectNode jsonNodes) throws Exception {

        Order order = null;
        try {
            long orderKey = jsonNodes.get("key").longValue();
            long rowNumber = jsonNodes.get("value").get("rowNumber").longValue();
            long customerKey = jsonNodes.get("value").get("customerKey").longValue();
            char orderStatus = (char) jsonNodes.get("value").get("orderStatus").asInt();
            long totalPrice = Math.round(jsonNodes.get("value").get("totalPrice").asDouble());
            int orderDate = jsonNodes.get("value").get("orderDate").asInt();
            String orderPriority = jsonNodes.get("value").get("orderPriority").asText();
            String clerk = jsonNodes.get("value").get("clerk").asText();
            int shipPriority = jsonNodes.get("value").get("shipPriority").asInt();
            String comment = jsonNodes.get("value").get("comment").asText();
            order = new Order(rowNumber, orderKey, customerKey, orderStatus, totalPrice, orderDate, orderPriority,
                    clerk, shipPriority, comment);
        } catch (NumberFormatException nfe) {
            System.out.println("Invalid record: " + jsonNodes + " exception: ");
            nfe.printStackTrace();
        } catch (Exception e) {
            System.out.println("Invalid record: " + jsonNodes + " exception: ");
            e.printStackTrace();
        }
        return order;
    }
}

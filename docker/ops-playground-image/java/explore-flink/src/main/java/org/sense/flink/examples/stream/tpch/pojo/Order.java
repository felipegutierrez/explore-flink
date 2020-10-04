package org.sense.flink.examples.stream.tpch.pojo;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;

public class Order extends io.airlift.tpch.Order implements Serializable {
    private static final long serialVersionUID = 1L;

    public Order(long rowNumber, long orderKey, long customerKey, char orderStatus, long totalPrice, int orderDate,
                 String orderPriority, String clerk, int shipPriority, String comment) {
        super(rowNumber, orderKey, customerKey, orderStatus, totalPrice, orderDate, orderPriority, clerk, shipPriority,
                comment);
    }

    public String getValue() {
        return getRowNumber() + "|" + getOrderKey() + "|" + getCustomerKey() + "|" + getOrderStatus() + "|" + getTotalPrice()
                + "|" + getTotalPriceInCents() + "|" + getOrderDate()
                + "|" + getOrderPriority() + "|" + getClerk() + "|"
                + getShipPriority() + "|" + getComment();
    }

    public ObjectNode getJson() {
        ObjectNode order = JsonNodeFactory.instance.objectNode();
        order.put("rowNumber", getRowNumber());
        order.put("orderKey", getOrderKey());
        order.put("customerKey", getCustomerKey());
        order.put("orderStatus", getOrderStatus());
        order.put("totalPrice", getTotalPrice());
        order.put("totalPriceInCents", getTotalPriceInCents());
        order.put("orderDate", getOrderDate());
        order.put("orderPriority", getOrderPriority());
        order.put("clerk", getClerk());
        order.put("shipPriority", getShipPriority());
        order.put("comment", getComment());
        return order;
    }

    @Override
    public String toString() {
        return "Order [getRowNumber()=" + getRowNumber() + ", getOrderKey()=" + getOrderKey() + ", getCustomerKey()="
                + getCustomerKey() + ", getOrderStatus()=" + getOrderStatus() + ", getTotalPrice()=" + getTotalPrice()
                + ", getTotalPriceInCents()=" + getTotalPriceInCents() + ", getOrderDate()=" + getOrderDate()
                + ", getOrderPriority()=" + getOrderPriority() + ", getClerk()=" + getClerk() + ", getShipPriority()="
                + getShipPriority() + ", getComment()=" + getComment() + "]";
    }
}

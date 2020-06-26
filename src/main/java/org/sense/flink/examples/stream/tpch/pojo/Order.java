package org.sense.flink.examples.stream.tpch.pojo;

import java.io.Serializable;
import java.util.Date;

public class Order extends io.airlift.tpch.Order implements Serializable {
	private static final long serialVersionUID = 1L;
	private long timestamp;

	public Order(long rowNumber, long orderKey, long customerKey, char orderStatus, long totalPrice, int orderDate,
			String orderPriority, String clerk, int shipPriority, String comment) {
		super(rowNumber, orderKey, customerKey, orderStatus, totalPrice, orderDate, orderPriority, clerk, shipPriority,
				comment);
		this.timestamp = new Date().getTime();
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "Order [getTimestamp()=" + getTimestamp() + ", getRowNumber()=" + getRowNumber() + ", getOrderKey()="
				+ getOrderKey() + ", getCustomerKey()=" + getCustomerKey() + ", getOrderStatus()=" + getOrderStatus()
				+ ", getTotalPrice()=" + getTotalPrice() + ", getTotalPriceInCents()=" + getTotalPriceInCents()
				+ ", getOrderDate()=" + getOrderDate() + ", getOrderPriority()=" + getOrderPriority() + ", getClerk()="
				+ getClerk() + ", getShipPriority()=" + getShipPriority() + ", getComment()=" + getComment() + "]";
	}

}

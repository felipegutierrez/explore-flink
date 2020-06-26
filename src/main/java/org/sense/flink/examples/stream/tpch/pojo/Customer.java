package org.sense.flink.examples.stream.tpch.pojo;

import java.io.Serializable;
import java.util.Date;

public class Customer extends io.airlift.tpch.Customer implements Serializable {
	private static final long serialVersionUID = 1L;
	private long timestamp;

	public Customer(long rowNumber, long customerKey, String name, String address, long nationKey, String phone,
			long accountBalance, String marketSegment, String comment) {
		super(rowNumber, customerKey, name, address, nationKey, phone, accountBalance, marketSegment, comment);
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
		return "Customer [getTimestamp()=" + getTimestamp() + ", getRowNumber()=" + getRowNumber()
				+ ", getCustomerKey()=" + getCustomerKey() + ", getName()=" + getName() + ", getAddress()="
				+ getAddress() + ", getNationKey()=" + getNationKey() + ", getPhone()=" + getPhone()
				+ ", getAccountBalance()=" + getAccountBalance() + ", getAccountBalanceInCents()="
				+ getAccountBalanceInCents() + ", getMarketSegment()=" + getMarketSegment() + ", getComment()="
				+ getComment() + "]";
	}

}

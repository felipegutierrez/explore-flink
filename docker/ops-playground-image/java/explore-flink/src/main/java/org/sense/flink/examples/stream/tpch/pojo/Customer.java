package org.sense.flink.examples.stream.tpch.pojo;

import java.io.Serializable;

public class Customer extends io.airlift.tpch.Customer implements Serializable {
	private static final long serialVersionUID = 1L;

	public Customer(long rowNumber, long customerKey, String name, String address, long nationKey, String phone,
			long accountBalance, String marketSegment, String comment) {
		super(rowNumber, customerKey, name, address, nationKey, phone, accountBalance, marketSegment, comment);
	}

	@Override
	public String toString() {
		return "Customer [getRowNumber()=" + getRowNumber() + ", getCustomerKey()=" + getCustomerKey() + ", getName()="
				+ getName() + ", getAddress()=" + getAddress() + ", getNationKey()=" + getNationKey() + ", getPhone()="
				+ getPhone() + ", getAccountBalance()=" + getAccountBalance() + ", getAccountBalanceInCents()="
				+ getAccountBalanceInCents() + ", getMarketSegment()=" + getMarketSegment() + ", getComment()="
				+ getComment() + "]";
	}
}

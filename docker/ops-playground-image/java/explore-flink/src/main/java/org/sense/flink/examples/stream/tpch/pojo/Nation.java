package org.sense.flink.examples.stream.tpch.pojo;

import java.io.Serializable;

public class Nation extends io.airlift.tpch.Nation implements Serializable {
	private static final long serialVersionUID = 1L;

	public Nation(long rowNumber, long nationKey, String name, long regionKey, String comment) {
		super(rowNumber, nationKey, name, regionKey, comment);
	}

	@Override
	public String toString() {
		return "Nation [getRowNumber()=" + getRowNumber() + ", getNationKey()=" + getNationKey() + ", getName()="
				+ getName() + ", getRegionKey()=" + getRegionKey() + ", getComment()=" + getComment() + "]";
	}
}

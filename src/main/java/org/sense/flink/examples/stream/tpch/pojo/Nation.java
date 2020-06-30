package org.sense.flink.examples.stream.tpch.pojo;

import java.io.Serializable;
import java.util.Date;

public class Nation extends io.airlift.tpch.Nation implements Serializable {
	private static final long serialVersionUID = 1L;
	private long timestamp;

	public Nation(long rowNumber, long nationKey, String name, long regionKey, String comment) {
		super(rowNumber, nationKey, name, regionKey, comment);
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
		return "Nation [getTimestamp()=" + getTimestamp() + ", getRowNumber()=" + getRowNumber() + ", getNationKey()="
				+ getNationKey() + ", getName()=" + getName() + ", getRegionKey()=" + getRegionKey() + ", getComment()="
				+ getComment() + "]";
	}
}

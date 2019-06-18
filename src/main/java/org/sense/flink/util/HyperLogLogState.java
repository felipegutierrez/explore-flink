package org.sense.flink.util;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

public class HyperLogLogState implements Serializable {
	private static final long serialVersionUID = -8562159355843952620L;
	private HyperLogLog hyperLogLog;
	private Set<Object> values;
	private long lastTimer;

	public HyperLogLogState() {
		this.hyperLogLog = new HyperLogLog(16);
		this.lastTimer = 0L;
		this.values = new HashSet<Object>();
	}

	public HyperLogLog getHyperLogLog() {
		return hyperLogLog;
	}

	public void setHyperLogLog(HyperLogLog hyperLogLog) {
		this.hyperLogLog = hyperLogLog;
	}

	public long getLastTimer() {
		return lastTimer;
	}

	public void setLastTimer(long lastTimer) {
		this.lastTimer = lastTimer;
	}

	public void offer(Object o) {
		this.hyperLogLog.offer(o);
		this.values.add(o);
	}

	public long cardinality() {
		return this.hyperLogLog.cardinality();
	}

	public Set<Object> getValues() {
		return values;
	}
}
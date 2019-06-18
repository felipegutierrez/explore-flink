package org.sense.flink.util;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

public class HyperLogLogPlusState implements Serializable {
	private static final long serialVersionUID = 1871460310811356998L;
	private HyperLogLogPlus hyperLogLogPlus;
	private Set<Object> values;
	private long lastTimer;

	public HyperLogLogPlusState() {
		this.hyperLogLogPlus = new HyperLogLogPlus(16);
		this.lastTimer = 0L;
		this.values = new HashSet<Object>();
	}

	public HyperLogLogPlus getHyperLogLogPlus() {
		return hyperLogLogPlus;
	}

	public void setHyperLogLogPlus(HyperLogLogPlus hyperLogLog) {
		this.hyperLogLogPlus = hyperLogLog;
	}

	public long getLastTimer() {
		return lastTimer;
	}

	public void setLastTimer(long lastTimer) {
		this.lastTimer = lastTimer;
	}

	public void offer(Object o) {
		this.hyperLogLogPlus.offer(o);
		this.values.add(o);
	}

	public long cardinality() {
		return this.hyperLogLogPlus.cardinality();
	}

	public Set<Object> getValues() {
		return values;
	}
}
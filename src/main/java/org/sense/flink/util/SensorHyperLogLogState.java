package org.sense.flink.util;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

public class SensorHyperLogLogState implements Serializable {
	private static final long serialVersionUID = -5583305703449247520L;
	private DecimalFormat dec = new DecimalFormat("#0.0000000000");
	private HyperLogLog hyperLogLog;
	private Set<Object> values;
	private long lastTimer;
	// sensorId, sensorType, platformId, platformType, stationId, timestamp,
	// estimative, real, error
	private Tuple9<Integer, String, Integer, String, Integer, Long, Long, Long, String> value;

	public SensorHyperLogLogState() {
		this.hyperLogLog = new HyperLogLog(16);
		this.lastTimer = 0L;
		this.values = new HashSet<Object>();
		this.value = Tuple9.of(0, "", 0, "", 0, 0L, 0L, 0L, "");
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

	public void offer(Object o, Tuple8<Integer, String, Integer, String, Integer, Long, Double, String> tuple) {
		this.hyperLogLog.offer(o);
		this.values.add(o);
		this.value = Tuple9.of(tuple.f0, tuple.f1, tuple.f2, tuple.f3, tuple.f4, tuple.f5, 0L, 0L, "");
	}

	public long cardinality() {
		return this.hyperLogLog.cardinality();
	}

	public Set<Object> getValues() {
		return values;
	}

	public Tuple9<Integer, String, Integer, String, Integer, Long, Long, Long, String> getEstimative() {
		long estimative = cardinality();
		long real = getValues().size();
		// error (= [estimated cardinality - true cardinality] / true cardinality)
		double error = (double) (((double) (estimative - real)) / real);
		// System.out.println("platform[" + this.platform + "] Cardinality:\n estimate:
		// " + estimative + " real: " + real + " error: " + dec.format(error) + "%");
		this.value.f6 = estimative;
		this.value.f7 = real;
		this.value.f8 = dec.format(error) + "%";
		return this.value;
	}
}

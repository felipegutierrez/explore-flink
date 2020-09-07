package org.sense.flink.examples.stream.tpch.udf;

import java.util.BitSet;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.sense.flink.util.CpuGauge;

import net.openhft.affinity.impl.LinuxJNAAffinity;

public class Tuple6ToStringMap
		extends RichMapFunction<Tuple6<Integer, String, String, String, Double, Double>, String> {
	private static final long serialVersionUID = 1L;

	private transient CpuGauge cpuGauge;
	private BitSet affinity;
	private boolean pinningPolicy;

	public Tuple6ToStringMap(boolean pinningPolicy) {
		this.pinningPolicy = pinningPolicy;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		this.cpuGauge = new CpuGauge();
		getRuntimeContext().getMetricGroup().gauge("cpu", cpuGauge);

		if (this.pinningPolicy) {
			// listing the cpu cores available
			int nbits = Runtime.getRuntime().availableProcessors();
			// pinning operator' thread to a specific cpu core
			this.affinity = new BitSet(nbits);
			affinity.set(((int) Thread.currentThread().getId() % nbits));
			LinuxJNAAffinity.INSTANCE.setAffinity(affinity);
		}
	}

	@Override
	public String map(Tuple6<Integer, String, String, String, Double, Double> value) throws Exception {
		// updates the CPU core current in use
		this.cpuGauge.updateValue(LinuxJNAAffinity.INSTANCE.getCpu());

		String result = "customer (with nation) with revenue (custkey[" + value.f0 + "], name[" + value.f1
				+ "], address[" + value.f2 + "], nationname[" + value.f3 + "], acctbal[" + value.f4 + "], revenue["
				+ value.f5 + "])";
		return result;
	}
}

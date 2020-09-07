package org.sense.flink.examples.stream.tpch.udf;

import java.util.BitSet;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.util.CpuGauge;

import com.google.common.collect.ImmutableMap;

import net.openhft.affinity.impl.LinuxJNAAffinity;

public class JoinCustomerWithRevenueKeyedProcessFunction extends
		KeyedProcessFunction<Tuple, Tuple2<Integer, Double>, Tuple6<Integer, String, String, String, Double, Double>> {
	private static final long serialVersionUID = 1L;

	private ImmutableMap<Integer, Tuple4<String, String, String, Double>> customerWithNationList = null;
	private transient CpuGauge cpuGauge;
	private BitSet affinity;
	private boolean pinningPolicy;

	public JoinCustomerWithRevenueKeyedProcessFunction(boolean pinningPolicy) {
		this.pinningPolicy = pinningPolicy;
	}

	@Override
	public void open(Configuration parameters) {
		try {
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
			customerWithNationList = ImmutableMap.copyOf(new CustomerSource().getCustomersWithNation());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void processElement(Tuple2<Integer, Double> value,
			KeyedProcessFunction<Tuple, Tuple2<Integer, Double>, Tuple6<Integer, String, String, String, Double, Double>>.Context ctx,
			Collector<Tuple6<Integer, String, String, String, Double, Double>> out) {
		try {
			// updates the CPU core current in use
			this.cpuGauge.updateValue(LinuxJNAAffinity.INSTANCE.getCpu());

			for (Map.Entry<Integer, Tuple4<String, String, String, Double>> customerWithNation : customerWithNationList
					.entrySet()) {
				if (customerWithNation.getKey().equals(value.f0)) {
					out.collect(Tuple6.of(customerWithNation.getKey(), customerWithNation.getValue().f0,
							customerWithNation.getValue().f1, customerWithNation.getValue().f2,
							customerWithNation.getValue().f3, value.f1));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

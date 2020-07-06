package org.sense.flink.examples.stream.tpch.udf;

import java.util.BitSet;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.util.CpuGauge;

import net.openhft.affinity.impl.LinuxJNAAffinity;

public class JoinCustomerWithRevenueKeyedProcessFunction extends
		KeyedProcessFunction<Tuple, Tuple2<Integer, Double>, Tuple6<Integer, String, String, String, Double, Double>> {
	private static final long serialVersionUID = 1L;

	private ListState<Tuple5<Integer, String, String, String, Double>> customerWithNationList = null;
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

			ListStateDescriptor<Tuple5<Integer, String, String, String, Double>> customerWithNationDescriptor = new ListStateDescriptor<Tuple5<Integer, String, String, String, Double>>(
					"customerWithNationState",
					TypeInformation.of(new TypeHint<Tuple5<Integer, String, String, String, Double>>() {
					}));
			customerWithNationList = getRuntimeContext().getListState(customerWithNationDescriptor);
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

			if (customerWithNationList != null && Iterators.size(customerWithNationList.get().iterator()) == 0) {
				CustomerSource customerSource = new CustomerSource();
				List<Tuple5<Integer, String, String, String, Double>> customerWithNation = customerSource
						.getCustomersWithNation();
				customerWithNationList.addAll(customerWithNation);
			}

			for (Tuple5<Integer, String, String, String, Double> customerWithNation : customerWithNationList.get()) {
				if (customerWithNation.f0.equals(value.f0)) {
					out.collect(Tuple6.of(customerWithNation.f0, customerWithNation.f1, customerWithNation.f2,
							customerWithNation.f3, customerWithNation.f4, value.f1));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

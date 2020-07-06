package org.sense.flink.examples.stream.tpch.udf;

import java.util.BitSet;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.examples.stream.tpch.pojo.ShippingPriorityItem;
import org.sense.flink.util.CpuGauge;

import net.openhft.affinity.impl.LinuxJNAAffinity;

public class ShippingPriorityKeyedProcessFunction
		extends KeyedProcessFunction<Long, ShippingPriorityItem, ShippingPriorityItem> {
	private static final long serialVersionUID = 1L;

	private ListState<Tuple2<Integer, Double>> lineItemList = null;
	private transient CpuGauge cpuGauge;
	private BitSet affinity;
	private boolean pinningPolicy;

	public ShippingPriorityKeyedProcessFunction(boolean pinningPolicy) {
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

		ListStateDescriptor<Tuple2<Integer, Double>> lineItemDescriptor = new ListStateDescriptor<>("lineItemState",
				TypeInformation.of(new TypeHint<Tuple2<Integer, Double>>() {
				}));
		lineItemList = getRuntimeContext().getListState(lineItemDescriptor);
	}

	@Override
	public void processElement(ShippingPriorityItem shippingPriorityItem,
			KeyedProcessFunction<Long, ShippingPriorityItem, ShippingPriorityItem>.Context context,
			Collector<ShippingPriorityItem> out) throws Exception {
		// updates the CPU core current in use
		this.cpuGauge.updateValue(LinuxJNAAffinity.INSTANCE.getCpu());

		if (lineItemList != null && Iterators.size(lineItemList.get().iterator()) == 0) {
			LineItemSource lineItemSource = new LineItemSource();
			List<Tuple2<Integer, Double>> lineItems = lineItemSource.getLineItemsRevenueByOrderKey();
			lineItemList.addAll(lineItems);
		}

		for (Tuple2<Integer, Double> lineItem : lineItemList.get()) {
			// System.out.println("LineItem: " + lineItem + " ShippingPriorityItem: " +
			// shippingPriorityItem);
			if (shippingPriorityItem != null
					&& (lineItem.f0.longValue() == shippingPriorityItem.getOrderkey().longValue())) {
				shippingPriorityItem.setRevenue(lineItem.f1);
				out.collect(shippingPriorityItem);
			}
		}
	}
}

package org.sense.flink.examples.stream.tpch.udf;

import java.util.BitSet;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.examples.stream.tpch.pojo.LineItem;
import org.sense.flink.examples.stream.tpch.pojo.ShippingPriorityItem;
import org.sense.flink.util.CpuGauge;

import net.openhft.affinity.impl.LinuxJNAAffinity;

public class ShippingPriorityKeyedProcessFunction
		extends KeyedProcessFunction<Long, ShippingPriorityItem, ShippingPriorityItem> {
	private static final long serialVersionUID = 1L;

	private ListState<LineItem> lineItemList = null;
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

		ListStateDescriptor<LineItem> lineItemDescriptor = new ListStateDescriptor<LineItem>("lineItemState",
				LineItem.class);
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
			List<LineItem> lineItems = lineItemSource.getLineItems();
			lineItemList.addAll(lineItems);
		}

		for (LineItem lineItem : lineItemList.get()) {
			// System.out.println("LineItem: " + lineItem);
			// System.out.println("ShippingPriorityItem: " + shippingPriorityItem);
			if (shippingPriorityItem != null && shippingPriorityItem.getOrderkey().equals(lineItem.getRowNumber())) {
				// System.out.println("ShippingPriorityProcessWindowFunction TRUE: " +
				// shippingPriorityItem);
				out.collect(shippingPriorityItem);
			}
		}
	}
}

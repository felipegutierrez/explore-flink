package org.sense.flink.examples.stream.tpch.udf;

import java.util.BitSet;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.sense.flink.examples.stream.tpch.pojo.LineItem;
import org.sense.flink.examples.stream.tpch.pojo.Order;
import org.sense.flink.util.CpuGauge;

import net.openhft.affinity.impl.LinuxJNAAffinity;

public class RevenueByCustomerProcessWindow
		extends ProcessWindowFunction<Order, Tuple2<Integer, Double>, Long, TimeWindow> {

	private static final long serialVersionUID = 1L;

	private ListState<LineItem> lineItemList = null;
	private transient CpuGauge cpuGauge;
	private BitSet affinity;
	private boolean pinningPolicy;

	public RevenueByCustomerProcessWindow(boolean pinningPolicy) {
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
	public void process(Long key,
			ProcessWindowFunction<Order, Tuple2<Integer, Double>, Long, TimeWindow>.Context context,
			Iterable<Order> input, Collector<Tuple2<Integer, Double>> out) throws Exception {
		// updates the CPU core current in use
		this.cpuGauge.updateValue(LinuxJNAAffinity.INSTANCE.getCpu());

		if (lineItemList != null && Iterators.size(lineItemList.get().iterator()) == 0) {
			LineItemSource lineItemSource = new LineItemSource();
			List<LineItem> lineItems = lineItemSource.getLineItems();
			lineItemList.addAll(lineItems);
		}

		for (LineItem lineItem : lineItemList.get()) {
			// System.out.println("LineItem: " + lineItem);
			for (Order order : input) {
				if (order.getOrderKey() == lineItem.getOrderKey()) {
					// LineItem: compute revenue and project out return flag
					// revenue per item = l_extendedprice * (1 - l_discount)
					Double revenue = lineItem.getExtendedPrice() * (1 - lineItem.getDiscount());
					Integer customerKey = (int) order.getCustomerKey();
					out.collect(Tuple2.of(customerKey, revenue));
				}
			}
		}
	}
}

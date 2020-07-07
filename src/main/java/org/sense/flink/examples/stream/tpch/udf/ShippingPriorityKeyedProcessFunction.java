package org.sense.flink.examples.stream.tpch.udf;

import java.util.BitSet;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.examples.stream.tpch.pojo.ShippingPriorityItem;
import org.sense.flink.util.CpuGauge;

import com.google.common.collect.ImmutableList;

import net.openhft.affinity.impl.LinuxJNAAffinity;

public class ShippingPriorityKeyedProcessFunction
		extends KeyedProcessFunction<Long, ShippingPriorityItem, ShippingPriorityItem> {
	private static final long serialVersionUID = 1L;

	private final ImmutableList<Tuple2<Integer, Double>> lineItemList = ImmutableList
			.copyOf(new LineItemSource().getLineItemsRevenueByOrderKey());
	private transient CpuGauge cpuGauge;
	private BitSet affinity;
	private boolean pinningPolicy;

	public ShippingPriorityKeyedProcessFunction(boolean pinningPolicy) {
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

			// LineItemSource lineItemSource = new LineItemSource();
			// lineItemList = lineItemSource.getLineItemsRevenueByOrderKey();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void processElement(ShippingPriorityItem shippingPriorityItem,
			KeyedProcessFunction<Long, ShippingPriorityItem, ShippingPriorityItem>.Context context,
			Collector<ShippingPriorityItem> out) {
		try {
			// updates the CPU core current in use
			this.cpuGauge.updateValue(LinuxJNAAffinity.INSTANCE.getCpu());

			for (Tuple2<Integer, Double> lineItem : lineItemList) {
				if (lineItem.f0.longValue() == shippingPriorityItem.getOrderkey().longValue()) {
					shippingPriorityItem.setRevenue(lineItem.f1);
					out.collect(shippingPriorityItem);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

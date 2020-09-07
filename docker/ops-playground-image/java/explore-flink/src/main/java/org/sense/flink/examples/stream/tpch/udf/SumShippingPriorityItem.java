package org.sense.flink.examples.stream.tpch.udf;

import java.util.BitSet;

import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.sense.flink.examples.stream.tpch.pojo.ShippingPriorityItem;
import org.sense.flink.util.CpuGauge;

import net.openhft.affinity.impl.LinuxJNAAffinity;

public class SumShippingPriorityItem extends RichReduceFunction<ShippingPriorityItem> {
	private static final long serialVersionUID = 1L;

	private transient CpuGauge cpuGauge;
	private BitSet affinity;
	private boolean pinningPolicy;

	public SumShippingPriorityItem(boolean pinningPolicy) {
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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public ShippingPriorityItem reduce(ShippingPriorityItem value1, ShippingPriorityItem value2) {
		ShippingPriorityItem shippingPriorityItem = null;
		try {
			// updates the CPU core current in use
			this.cpuGauge.updateValue(LinuxJNAAffinity.INSTANCE.getCpu());

			shippingPriorityItem = new ShippingPriorityItem(value1.getOrderkey(),
					value1.getRevenue() + value2.getRevenue(), value1.getOrderdate(), value1.getShippriority());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return shippingPriorityItem;
	}
}

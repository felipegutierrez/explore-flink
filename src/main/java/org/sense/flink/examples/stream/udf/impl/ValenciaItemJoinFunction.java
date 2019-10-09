package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.util.CpuGauge;

import net.openhft.affinity.impl.LinuxJNAAffinity;

public class ValenciaItemJoinFunction
		extends RichJoinFunction<ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>> {
	private static final long serialVersionUID = -5624248427888414054L;
	private transient CpuGauge cpuGauge;

	@Override
	public void open(Configuration config) {
		this.cpuGauge = new CpuGauge();
		getRuntimeContext().getMetricGroup().gauge("cpu", cpuGauge);
	}

	@Override
	public Tuple2<ValenciaItem, ValenciaItem> join(ValenciaItem first, ValenciaItem second) throws Exception {
		// updates the CPU core current in use
		this.cpuGauge.updateValue(LinuxJNAAffinity.INSTANCE.getCpu());

		return Tuple2.of(first, second);
	}
}

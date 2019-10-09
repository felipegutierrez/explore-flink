package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.sense.flink.pojo.ValenciaItemEnriched;
import org.sense.flink.util.CpuGauge;

import net.openhft.affinity.impl.LinuxJNAAffinity;

public class ValenciaItemEnrichedToStringMap extends RichMapFunction<ValenciaItemEnriched, String> {
	private static final long serialVersionUID = -595541172635167702L;

	private transient CpuGauge cpuGauge;

	@Override
	public void open(Configuration parameters) throws Exception {
		this.cpuGauge = new CpuGauge();
		getRuntimeContext().getMetricGroup().gauge("cpu", cpuGauge);
	}

	@Override
	public String map(ValenciaItemEnriched value) throws Exception {
		// updates the CPU core current in use
		this.cpuGauge.updateValue(LinuxJNAAffinity.INSTANCE.getCpu());

		return value.toString();
	}
}

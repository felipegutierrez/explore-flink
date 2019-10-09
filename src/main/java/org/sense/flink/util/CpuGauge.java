package org.sense.flink.util;

import org.apache.flink.metrics.Gauge;

public class CpuGauge implements Gauge<Integer> {

	private int cpuCore;

	@Override
	public Integer getValue() {
		return cpuCore;
	}

	public void updateValue(int cpuCore) {
		this.cpuCore = cpuCore;
	}
}

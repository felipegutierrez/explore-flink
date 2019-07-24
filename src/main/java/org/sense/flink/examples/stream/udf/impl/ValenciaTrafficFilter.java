package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.functions.FilterFunction;
import org.sense.flink.pojo.ValenciaTraffic;

public class ValenciaTrafficFilter implements FilterFunction<ValenciaTraffic> {
	private static final long serialVersionUID = -6208908624085519344L;
	private Integer[] status;

	/**
	 * Constructor with default values
	 */
	public ValenciaTrafficFilter() {
		this(new Integer[] { 1, 2, 3 });
	}

	public ValenciaTrafficFilter(Integer[] status) {
		this.status = status;
	}

	@Override
	public boolean filter(ValenciaTraffic value) throws Exception {
		boolean flag = false;
		for (int i = 0; i < status.length; i++) {
			if (value.getStatus().intValue() == this.status[i].intValue()) {
				flag = true;
			}

		}
		return flag;
	}
}

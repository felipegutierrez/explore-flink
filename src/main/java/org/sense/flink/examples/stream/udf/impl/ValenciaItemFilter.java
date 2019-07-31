package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.functions.FilterFunction;
import org.sense.flink.pojo.AirPollution;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.util.ValenciaItemType;

public class ValenciaItemFilter implements FilterFunction<ValenciaItem> {
	private static final long serialVersionUID = -2527092442773375276L;
	private ValenciaItemType valenciaItemType;
	private Integer[] status;
	private AirPollution threshold;

	/**
	 * Constructor with default values
	 */
	public ValenciaItemFilter(ValenciaItemType valenciaItemType) {
		this(valenciaItemType, new Integer[] { 1, 2, 3 }, new AirPollution());
	}

	public ValenciaItemFilter(ValenciaItemType valenciaItemType, Integer[] status, AirPollution threshold) {
		this.valenciaItemType = valenciaItemType;
		this.status = status;
		this.threshold = threshold;
	}

	@Override
	public boolean filter(ValenciaItem value) throws Exception {
		boolean flag = false;
		if (valenciaItemType == ValenciaItemType.TRAFFIC) {
			for (int i = 0; i < status.length; i++) {
				if (((Integer) value.getValue()).intValue() == this.status[i].intValue()) {
					// System.out.println(value);
					flag = true;
				}
			}
		} else if (valenciaItemType == ValenciaItemType.AIR_POLLUTION) {
			// @formatter:off
			AirPollution currentParameters = (AirPollution)value.getValue();
			if (currentParameters == null) {
				return false;
			}
			if ((currentParameters.getSo2() != null && currentParameters.getSo2().doubleValue() >= this.threshold.getSo2().doubleValue())
					|| (currentParameters.getCo() != null && currentParameters.getCo().doubleValue() >= this.threshold.getCo().doubleValue())
					|| (currentParameters.getOzono() != null && currentParameters.getOzono().doubleValue() >= this.threshold.getOzono().doubleValue())
					|| (currentParameters.getNox() != null && currentParameters.getNox().doubleValue() >= this.threshold.getNox().doubleValue())
					|| (currentParameters.getNo() != null && currentParameters.getNo().doubleValue() >= this.threshold.getNo().doubleValue())
					|| (currentParameters.getNo2() != null && currentParameters.getNo2().doubleValue() >= this.threshold.getNo2().doubleValue())
					|| (currentParameters.getBenc() != null && currentParameters.getBenc().doubleValue() >= this.threshold.getBenc().doubleValue())
					|| (currentParameters.getTolue() != null && currentParameters.getTolue().doubleValue() >= this.threshold.getTolue().doubleValue())
					|| (currentParameters.getXilen() != null && currentParameters.getXilen().doubleValue() >= this.threshold.getXilen().doubleValue())
					|| (currentParameters.getPm10() != null && currentParameters.getPm10().doubleValue() >= this.threshold.getPm10().doubleValue())
					|| (currentParameters.getPm2_5() != null && currentParameters.getPm2_5().doubleValue() >= this.threshold.getPm2_5().doubleValue())
					|| (currentParameters.getSpl() != null && currentParameters.getSpl().doubleValue() >= this.threshold.getSpl().doubleValue())) {
				// System.out.println(value);
				// currentParameters.compareAirPollution(this.threshold);
				return true;
			}
			// @formatter:on
		} else if (valenciaItemType == ValenciaItemType.NOISE) {
		} else {
			throw new Exception("ValenciaItemType is NULL!");
		}
		return flag;
	}
}

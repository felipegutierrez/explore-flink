package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.functions.FilterFunction;
import org.sense.flink.pojo.AirPollution;
import org.sense.flink.pojo.ValenciaPollution;

/**
 * This function filter values that are equal or above of a certain threshold.
 * Values that are below the threshold will be discarded from the stream.
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaPollutionFilter implements FilterFunction<ValenciaPollution> {
	private static final long serialVersionUID = 6044671792437478508L;
	private AirPollution threshold;

	/**
	 * Constructor with default values
	 */
	public ValenciaPollutionFilter() {
		this(new AirPollution());
	}

	public ValenciaPollutionFilter(AirPollution threshold) {
		this.threshold = threshold;
	}

	@Override
	public boolean filter(ValenciaPollution value) throws Exception {
		// @formatter:off
		AirPollution currentParameters = value.getParameters();
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
			currentParameters.compareAirPollution(this.threshold);
			return true;
		}
		return false;
		// @formatter:on
	}
}

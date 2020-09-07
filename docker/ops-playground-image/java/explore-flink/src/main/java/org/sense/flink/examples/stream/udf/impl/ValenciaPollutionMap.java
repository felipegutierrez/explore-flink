package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.functions.MapFunction;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.pojo.ValenciaPollution;

public class ValenciaPollutionMap implements MapFunction<ValenciaItem, ValenciaPollution> {
	private static final long serialVersionUID = -2800669148386626039L;

	@Override
	public ValenciaPollution map(ValenciaItem value) throws Exception {
		return (ValenciaPollution) value;
	}
}

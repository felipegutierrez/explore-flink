package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.sense.flink.pojo.ValenciaItemEnriched;

public class ValenciaItemEnrichedToStringMap extends RichMapFunction<ValenciaItemEnriched, String> {
	private static final long serialVersionUID = -595541172635167702L;

	@Override
	public String map(ValenciaItemEnriched value) throws Exception {
		return value.toString();
	}
}

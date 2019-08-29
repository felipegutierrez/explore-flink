package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.util.ValenciaItemType;

public class ValenciaItemSyntheticCoFlatMapper
		extends RichCoFlatMapFunction<ValenciaItem, Tuple2<ValenciaItemType, String>, ValenciaItem> {
	private static final long serialVersionUID = -1514774680153468836L;
	private MapState<ValenciaItemType, Integer> state;

	@Override
	public void open(Configuration parameters) throws Exception {
		MapStateDescriptor<ValenciaItemType, Integer> stateProperties = new MapStateDescriptor<ValenciaItemType, Integer>(
				"FactorState", ValenciaItemType.class, Integer.class);
		state = getRuntimeContext().getMapState(stateProperties);
	}

	@Override
	public void flatMap1(ValenciaItem value, Collector<ValenciaItem> out) throws Exception {

		ValenciaItemType valenciaItemType = value.getType();
		Integer factorTraffic = state.get(ValenciaItemType.TRAFFIC_JAM);
		Integer factorAirPollution = state.get(ValenciaItemType.AIR_POLLUTION);
		Integer factorNoise = state.get(ValenciaItemType.NOISE);

		if (factorTraffic != null && factorTraffic.intValue() > 1 && ValenciaItemType.TRAFFIC_JAM == valenciaItemType) {
			for (int i = 0; i < factorTraffic.intValue(); i++) {
				out.collect(value);
			}
		} else if (factorAirPollution != null && factorAirPollution.intValue() > 1
				&& ValenciaItemType.AIR_POLLUTION == valenciaItemType) {
			for (int i = 0; i < factorAirPollution.intValue(); i++) {
				out.collect(value);
			}
		} else if (factorNoise != null && factorNoise.intValue() > 1 && ValenciaItemType.NOISE == valenciaItemType) {
			for (int i = 0; i < factorNoise.intValue(); i++) {
				out.collect(value);
			}
		} else {
			out.collect(value);
		}
	}

	@Override
	public void flatMap2(Tuple2<ValenciaItemType, String> value, Collector<ValenciaItem> out) throws Exception {
		System.out.println(value);
		ValenciaItemType valenciaItemType = value.f0;
		Integer factor = Integer.valueOf(value.f1);

		if (valenciaItemType != ValenciaItemType.AIR_POLLUTION && valenciaItemType != ValenciaItemType.TRAFFIC_JAM
				&& valenciaItemType != ValenciaItemType.NOISE) {
			throw new Exception("ValenciaItemType invalid [" + valenciaItemType + "]!");
		}
		if (factor.intValue() == 1) {
			state.remove(valenciaItemType);
		} else {
			state.put(valenciaItemType, factor);
		}
	}
}

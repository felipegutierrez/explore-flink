package org.sense.flink.examples.stream.udf.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.sense.flink.mqtt.MqttMessage;
import org.sense.flink.util.ValenciaItemType;

public class ValenciaItemTypeParamMap extends RichMapFunction<MqttMessage, Tuple2<ValenciaItemType, String>> {
	private static final long serialVersionUID = -2098022729317434426L;

	@Override
	public Tuple2<ValenciaItemType, String> map(MqttMessage value) throws Exception {
		System.out.println(value);

		ValenciaItemType valenciaItemType = null;
		String factor = null;
		String payload = value.getPayload();
		String[] parameters = payload.split(" ");
		if (parameters == null || parameters.length != 2) {
			System.err.println("parameter invalid!");
		} else {
			String itemType = String.valueOf(parameters[0]);
			factor = String.valueOf(parameters[1]);
			if (ValenciaItemType.AIR_POLLUTION.toString().equals(itemType)) {
				valenciaItemType = ValenciaItemType.AIR_POLLUTION;
			} else if (ValenciaItemType.TRAFFIC_JAM.toString().equals(itemType)) {
				valenciaItemType = ValenciaItemType.TRAFFIC_JAM;
			} else if (ValenciaItemType.NOISE.toString().equals(itemType)) {
				valenciaItemType = ValenciaItemType.NOISE;
			} else {
				System.err.println("Invalid ValenciaItemType[" + itemType + "]!");
				return Tuple2.of(ValenciaItemType.NULL, "0");
			}
			if (!StringUtils.isNumeric(factor)) {
				System.err.println("Invalid frequency pull[" + factor + "]!");
				return Tuple2.of(valenciaItemType, "0");
			}
			if (Integer.parseInt(factor) < 1 || Integer.parseInt(factor) > 1000) {
				System.err.println("Invalid frequency pull (>1 or <1000) [" + factor + "]!");
				return Tuple2.of(valenciaItemType, "0");
			}
			return Tuple2.of(valenciaItemType, factor);
		}
		return Tuple2.of(valenciaItemType, "0");
	}
}

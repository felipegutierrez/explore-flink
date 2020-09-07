package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.sense.flink.mqtt.CompositeKeyStationPlatform;
import org.sense.flink.mqtt.MqttSensor;

public class StationPlatformKeySelector
		implements KeySelector<Tuple2<CompositeKeyStationPlatform, MqttSensor>, CompositeKeyStationPlatform> {
	private static final long serialVersionUID = -7306413191299903211L;

	@Override
	public CompositeKeyStationPlatform getKey(Tuple2<CompositeKeyStationPlatform, MqttSensor> value) throws Exception {
		return value.f0;
	}
}
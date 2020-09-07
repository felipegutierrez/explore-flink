package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.sense.flink.mqtt.CompositeKeySensorTypePlatformStation;
import org.sense.flink.mqtt.MqttSensor;

public class SensorKeySelector
		implements KeySelector<Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>, CompositeKeySensorTypePlatformStation> {
	private static final long serialVersionUID = -3602693589805184075L;

	@Override
	public CompositeKeySensorTypePlatformStation getKey(Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor> value) throws Exception {
		return value.f0;
	}
}
package org.sense.flink.examples.stream.udfs;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.sense.flink.mqtt.CompositeKeySensorType;
import org.sense.flink.mqtt.MqttSensor;

public class SensorKeySelector
		implements KeySelector<Tuple2<CompositeKeySensorType, MqttSensor>, CompositeKeySensorType> {
	private static final long serialVersionUID = -1850482738358805000L;

	@Override
	public CompositeKeySensorType getKey(Tuple2<CompositeKeySensorType, MqttSensor> value) throws Exception {
		return value.f0;
	}
}
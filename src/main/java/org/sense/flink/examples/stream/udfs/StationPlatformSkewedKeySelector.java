package org.sense.flink.examples.stream.udfs;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.sense.flink.mqtt.CompositeSkewedKeyStationPlatform;
import org.sense.flink.mqtt.MqttSensor;

public class StationPlatformSkewedKeySelector implements
		KeySelector<Tuple2<CompositeSkewedKeyStationPlatform, MqttSensor>, CompositeSkewedKeyStationPlatform> {
	private static final long serialVersionUID = -8089623219760351803L;

	@Override
	public CompositeSkewedKeyStationPlatform getKey(Tuple2<CompositeSkewedKeyStationPlatform, MqttSensor> value)
			throws Exception {
		return value.f0;
	}
}
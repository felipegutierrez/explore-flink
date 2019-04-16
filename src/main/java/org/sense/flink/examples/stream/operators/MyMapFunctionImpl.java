package org.sense.flink.examples.stream.operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.sense.flink.mqtt.CompositeKeyStationPlatform;
import org.sense.flink.mqtt.MqttSensor;

public class MyMapFunctionImpl implements
		MyMapFunction<Tuple2<CompositeKeyStationPlatform, MqttSensor>, Tuple2<CompositeKeyStationPlatform, MqttSensor>> {

	private static final long serialVersionUID = -1168133355176644948L;

	@Override
	public Tuple2<CompositeKeyStationPlatform, MqttSensor> map(Tuple2<CompositeKeyStationPlatform, MqttSensor> value)
			throws Exception {
		System.out.println("TestMyMapFunction: " + value);
		return value;
	}
}
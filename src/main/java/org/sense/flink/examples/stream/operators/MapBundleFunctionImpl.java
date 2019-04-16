package org.sense.flink.examples.stream.operators;

import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.sense.flink.mqtt.CompositeKeySensorTypePlatformStation;
import org.sense.flink.mqtt.MqttSensor;

public class MapBundleFunctionImpl extends
		MapBundleFunction<CompositeKeySensorTypePlatformStation, MqttSensor, Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>, MqttSensor> {

	private static final long serialVersionUID = -1168133355176644948L;

	/*
	 * @Override public Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>
	 * map(Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor> input) throws
	 * Exception { System.out.println("TestMyMapFunction: " + input); return input;
	 * }
	 */

	@Override
	public MqttSensor addInput(MqttSensor value, Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor> input) {
		if (value == null) {
			return input.f1;
		} else {
			// pre-aggregate values
			MqttSensor currentInput = input.f1;

			Long timestamp = currentInput.getTimestamp() > value.getTimestamp() ? currentInput.getTimestamp()
					: value.getTimestamp();
			Double v = value.getValue() + currentInput.getValue();
			String trip = value.getTrip() + "|" + currentInput.getTrip();

			value.setTimestamp(timestamp);
			value.setValue(v);
			value.setTrip(trip);
			return value;
		}
	}

	@Override
	public void finishBundle(Map<CompositeKeySensorTypePlatformStation, MqttSensor> buffer, Collector<MqttSensor> out)
			throws Exception {
		finishCount++;
		outputs.clear();
		for (Map.Entry<CompositeKeySensorTypePlatformStation, MqttSensor> entry : buffer.entrySet()) {
			outputs.add(entry.getValue());
		}
	}
}
package org.sense.flink.examples.stream.operators;

import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.sense.flink.mqtt.CompositeKeySensorTypePlatformStation;
import org.sense.flink.mqtt.MqttSensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapBundleFunctionImpl extends
		MapBundleFunction<CompositeKeySensorTypePlatformStation, MqttSensor, Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>, MqttSensor> {

	private static final Logger logger = LoggerFactory.getLogger(MapBundleFunctionImpl.class);
	private static final long serialVersionUID = -1168133355176644948L;

	@Override
	public MqttSensor addInput(MqttSensor value, Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor> input) {
		if (value == null) {
			return input.f1;
		} else {
			// pre-aggregate values
			MqttSensor currentInput = input.f1;

			// check if keys are equal
			if (!currentInput.getKey().f1.equals(value.getKey().f1)
					|| !currentInput.getKey().f2.equals(value.getKey().f2)
					|| !currentInput.getKey().f4.equals(value.getKey().f4)) {
				logger.error("Keys are not equal [" + currentInput + "] - [" + value + "]");
			}

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
			out.collect(entry.getValue());
		}
	}
}
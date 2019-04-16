package org.sense.flink.examples.stream.udfs;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.sense.flink.mqtt.CompositeKeySensorTypePlatformStation;
import org.sense.flink.mqtt.MqttSensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SensorTypeReduce implements ReduceFunction<Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>> {
	private static final long serialVersionUID = -6992113920095082919L;
	private static final Logger logger = LoggerFactory.getLogger(SensorTypeReduce.class);

	@Override
	public Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor> reduce(Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor> value1,
			Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor> value2) throws Exception {

		Tuple5<Integer, String, Integer, String, Integer> key = value1.f1.getKey();
		key.f0 = 0;
		Double sum = value1.f1.getValue() + value2.f1.getValue();

		// System.out.println("Reducer: " + sum);
		return Tuple2.of(value1.f0, new MqttSensor("", key, System.currentTimeMillis(), sum, value1.f1.getTrip()));
	}
}

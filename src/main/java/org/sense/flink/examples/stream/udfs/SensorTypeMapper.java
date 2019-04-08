package org.sense.flink.examples.stream.udfs;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
// import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
// import org.apache.flink.metrics.Meter;
import org.sense.flink.mqtt.CompositeKeySensorType;
import org.sense.flink.mqtt.MqttSensor;

public class SensorTypeMapper extends RichMapFunction<MqttSensor, Tuple2<CompositeKeySensorType, MqttSensor>> {

	private static final long serialVersionUID = 220085096993170929L;
	// Create metrics
	// private transient Counter counter;
	// private transient Meter meter;

	@Override
	public void open(Configuration config) throws Exception {
		// this.counter=getRuntimeContext().getMetricGroup().counter("counterSensorTypeMapper");
		// com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
		// this.meter = getRuntimeContext().getMetricGroup().meter(SensorTypeMapper.class.getSimpleName() + "-meter",
		// new DropwizardMeterWrapper(dropwizardMeter));
	}

	@Override
	public Tuple2<CompositeKeySensorType, MqttSensor> map(MqttSensor value) throws Exception {
		// this.meter.markEvent();
		// this.counter.inc();
		// every sensor key: sensorId, sensorType, platformId, platformType, stationId
		// Integer sensorId = value.getKey().f0;
		String sensorType = value.getKey().f1;
		Integer platformId = value.getKey().f2;
		// String platformType = value.getKey().f3;
		Integer stationId = value.getKey().f4;
		CompositeKeySensorType compositeKey = new CompositeKeySensorType(stationId, platformId, sensorType);

		// System.out.println("Mapper: " + compositeKey + " - " + value);

		return Tuple2.of(compositeKey, value);
	}
}

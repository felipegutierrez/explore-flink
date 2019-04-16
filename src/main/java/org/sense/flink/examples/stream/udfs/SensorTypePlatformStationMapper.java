package org.sense.flink.examples.stream.udfs;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.sense.flink.mqtt.CompositeKeySensorTypePlatformStation;
import org.sense.flink.mqtt.MqttSensor;

public class SensorTypePlatformStationMapper
		extends RichMapFunction<MqttSensor, Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>> {

	private static final long serialVersionUID = 220085096993170929L;
	// Create metrics
	// private transient Counter counter;
	private transient Meter meter;
	private String metricName;

	public SensorTypePlatformStationMapper() {
		this.metricName = SensorTypePlatformStationMapper.class.getSimpleName();
	}

	public SensorTypePlatformStationMapper(String metricName) {
		this.metricName = metricName;
	}

	@Override
	public void open(Configuration config) throws Exception {
		// this.counter=getRuntimeContext().getMetricGroup().counter(this.metricName+"counter");
		com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
		this.meter = getRuntimeContext().getMetricGroup().meter(this.metricName + "-meter",
				new DropwizardMeterWrapper(dropwizardMeter));
	}

	@Override
	public Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor> map(MqttSensor value) throws Exception {
		this.meter.markEvent();
		// this.counter.inc();

		// Integer sensorId = value.getKey().f0;
		String sensorType = value.getKey().f1;
		Integer platformId = value.getKey().f2;
		// String platformType = value.getKey().f3;
		Integer stationId = value.getKey().f4;
		CompositeKeySensorTypePlatformStation compositeKey = new CompositeKeySensorTypePlatformStation(stationId, platformId, sensorType);

		// System.out.println("Mapper: " + compositeKey + " - " + value);
		return Tuple2.of(compositeKey, value);
	}
}

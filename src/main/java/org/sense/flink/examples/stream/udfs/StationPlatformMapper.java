package org.sense.flink.examples.stream.udfs;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.sense.flink.mqtt.CompositeKeyStationPlatform;
import org.sense.flink.mqtt.MqttSensor;

public class StationPlatformMapper
		extends RichMapFunction<MqttSensor, Tuple2<CompositeKeyStationPlatform, MqttSensor>> {

	private static final long serialVersionUID = -5933537289987970547L;
	// Create metrics
	// private transient Counter counter;
	private transient Meter meter;
	private String metricName;

	public StationPlatformMapper() {
		this.metricName = StationPlatformMapper.class.getSimpleName();
	}

	public StationPlatformMapper(String metricName) {
		this.metricName = metricName;
	}

	@Override
	public void open(Configuration config) throws Exception {
		// this.counter=getRuntimeContext().getMetricGroup().counter("counterSensorTypeMapper");
		com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();

		this.meter = getRuntimeContext().getMetricGroup().meter(this.metricName + "-meter",
				new DropwizardMeterWrapper(dropwizardMeter));
	}

	@Override
	public Tuple2<CompositeKeyStationPlatform, MqttSensor> map(MqttSensor value) throws Exception {
		this.meter.markEvent();
		// this.counter.inc();
		// every sensor key: sensorId, sensorType, platformId, platformType, stationId
		// Integer sensorId = value.getKey().f0;
		// String sensorType = value.getKey().f1;
		Integer platformId = value.getKey().f2;
		// String platformType = value.getKey().f3;
		Integer stationId = value.getKey().f4;
		CompositeKeyStationPlatform compositeKey = new CompositeKeyStationPlatform(stationId, platformId);
		// System.out.println("Mapper: " + compositeKey + " - " + value);

		return Tuple2.of(compositeKey, value);
	}
}

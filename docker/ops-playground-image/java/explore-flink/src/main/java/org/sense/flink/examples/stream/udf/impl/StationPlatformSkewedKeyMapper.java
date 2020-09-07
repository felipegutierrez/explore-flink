package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.sense.flink.mqtt.CompositeSkewedKeyStationPlatform;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.util.SkewParameterGenerator;

public class StationPlatformSkewedKeyMapper
		extends RichMapFunction<MqttSensor, Tuple2<CompositeSkewedKeyStationPlatform, MqttSensor>> {

	private static final long serialVersionUID = -5845625963018367785L;
	// Create metrics
	// private transient Counter counter;
	private transient Meter meter;
	private String metricName;
	private SkewParameterGenerator skewParameterGenerator;

	public StationPlatformSkewedKeyMapper() {
		this.metricName = StationPlatformSkewedKeyMapper.class.getSimpleName();
		this.skewParameterGenerator = new SkewParameterGenerator(10);
	}

	public StationPlatformSkewedKeyMapper(String metricName) {
		this.metricName = metricName;
		this.skewParameterGenerator = new SkewParameterGenerator(10);
	}

	@Override
	public void open(Configuration config) throws Exception {
		// this.counter=getRuntimeContext().getMetricGroup().counter("counterSensorTypeMapper");
		com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();

		this.meter = getRuntimeContext().getMetricGroup().meter(this.metricName + "-meter",
				new DropwizardMeterWrapper(dropwizardMeter));
	}

	@Override
	public Tuple2<CompositeSkewedKeyStationPlatform, MqttSensor> map(MqttSensor value) throws Exception {
		this.meter.markEvent();
		// this.counter.inc();
		// every sensor key: sensorId, sensorType, platformId, platformType, stationId
		// Integer sensorId = value.getKey().f0;
		// String sensorType = value.getKey().f1;
		Integer platformId = value.getKey().f2;
		// String platformType = value.getKey().f3;
		Integer stationId = value.getKey().f4;
		Integer skewParameter = 0;

		if (stationId.equals(new Integer(2)) && platformId.equals(new Integer(3))) {
			System.out.println("this is our skewed key: [station, platform]:[" + stationId + "," + platformId + "]");
			skewParameter = this.skewParameterGenerator.getNextItem();
		}
		CompositeSkewedKeyStationPlatform compositeKey = new CompositeSkewedKeyStationPlatform(stationId, platformId,
				skewParameter);
		// System.out.println("Mapper: " + compositeKey + " - " + value);

		return Tuple2.of(compositeKey, value);
	}
}

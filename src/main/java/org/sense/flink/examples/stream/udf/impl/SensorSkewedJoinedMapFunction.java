package org.sense.flink.examples.stream.udf.impl;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.sense.flink.mqtt.CompositeKeyStationPlatform;
import org.sense.flink.mqtt.MqttSensor;

public class SensorSkewedJoinedMapFunction
		extends RichMapFunction<Tuple3<CompositeKeyStationPlatform, MqttSensor, MqttSensor>, String> {
	private static final long serialVersionUID = -1805802240577265810L;
	private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private transient Meter meter;

	@Override
	public void open(Configuration config) throws Exception {
		// this.counter=getRuntimeContext().getMetricGroup().counter("counterSensorTypeMapper");
		com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
		this.meter = getRuntimeContext().getMetricGroup().meter(
				SensorSkewedJoinedMapFunction.class.getSimpleName() + "-meter",
				new DropwizardMeterWrapper(dropwizardMeter));
	}

	@Override
	public String map(Tuple3<CompositeKeyStationPlatform, MqttSensor, MqttSensor> value) throws Exception {
		this.meter.markEvent();
		// this.counter.inc();

		String key = "key[Station: " + value.f0.getStationId() + ", Platform: " + value.f0.getPlatformId() + "]";
		String result01 = "[" + value.f1.getKey().f1 + "," + sdf.format(new Date(value.f1.getTimestamp())) + ","
				+ value.f1.getTrip() + "]";
		String result02 = "[" + value.f2.getKey().f1 + "," + sdf.format(new Date(value.f2.getTimestamp())) + ","
				+ value.f2.getTrip() + "]";

		return key + result01 + result02;
	}
}

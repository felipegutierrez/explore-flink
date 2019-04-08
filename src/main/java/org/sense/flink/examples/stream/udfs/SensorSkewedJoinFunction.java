package org.sense.flink.examples.stream.udfs;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.sense.flink.mqtt.CompositeKeyStationPlatform;
import org.sense.flink.mqtt.MqttSensor;

public class SensorSkewedJoinFunction extends
		RichJoinFunction<Tuple2<CompositeKeyStationPlatform, MqttSensor>, Tuple2<CompositeKeyStationPlatform, MqttSensor>, String> {
	private static final long serialVersionUID = 2507555650596738022L;
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	private transient Meter meter;

	@Override
	public void open(Configuration config) throws Exception {
		// this.counter=getRuntimeContext().getMetricGroup().counter("counterSensorTypeMapper");
		com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
		this.meter = getRuntimeContext().getMetricGroup().meter(
				SensorSkewedJoinFunction.class.getSimpleName() + "-meter", new DropwizardMeterWrapper(dropwizardMeter));
	}

	@Override
	public String join(Tuple2<CompositeKeyStationPlatform, MqttSensor> first,
			Tuple2<CompositeKeyStationPlatform, MqttSensor> second) throws Exception {
		this.meter.markEvent();
		// this.counter.inc();
		Integer stationId = first.f0.getStationId();
		Integer platformId = first.f0.getPlatformId();
		String key = "station,platform: [" + stationId + "," + platformId + "] ";

		String firstValue = first.f1.getKey().f1 + "," + first.f1.getTrip() + "," + first.f1.getValue() + ","
				+ sdf.format(new Date(first.f1.getTimestamp()));
		String secondValue = second.f1.getKey().f1 + "," + second.f1.getTrip() + "," + second.f1.getValue() + ","
				+ sdf.format(new Date(second.f1.getTimestamp()));

		return key + "-First: [" + firstValue + "] - Second: [" + secondValue + "]";
	}
}

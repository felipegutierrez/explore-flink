package org.sense.flink.examples.stream.udfs;

import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.sense.flink.mqtt.CompositeKeyStationPlatform;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.util.ProcessSomeStuff;

public class SensorSkewedJoinFunction extends
		RichJoinFunction<Tuple2<CompositeKeyStationPlatform, MqttSensor>, Tuple2<CompositeKeyStationPlatform, MqttSensor>, Tuple3<CompositeKeyStationPlatform, MqttSensor, MqttSensor>> {
	private static final long serialVersionUID = 2507555650596738022L;
	private transient Meter meter;

	@Override
	public void open(Configuration config) throws Exception {
		// this.counter=getRuntimeContext().getMetricGroup().counter("counterSensorTypeMapper");
		com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
		this.meter = getRuntimeContext().getMetricGroup().meter(
				SensorSkewedJoinFunction.class.getSimpleName() + "-meter", new DropwizardMeterWrapper(dropwizardMeter));
	}

	@Override
	public Tuple3<CompositeKeyStationPlatform, MqttSensor, MqttSensor> join(
			Tuple2<CompositeKeyStationPlatform, MqttSensor> first,
			Tuple2<CompositeKeyStationPlatform, MqttSensor> second) throws Exception {
		this.meter.markEvent();
		// this.counter.inc();

		CompositeKeyStationPlatform key = first.f0;
		MqttSensor firstValue = first.f1;
		MqttSensor secondValue = second.f1;

		ProcessSomeStuff.processSomeStuff(key, 5);

		return Tuple3.of(key, firstValue, secondValue);
	}
}

package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.sense.flink.mqtt.CompositeKeyStationPlatform;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.util.ProcessSomeStuff;

public class StationPlatformRichWindowFunction extends
		RichWindowFunction<Tuple2<CompositeKeyStationPlatform, MqttSensor>, MqttSensor, CompositeKeyStationPlatform, TimeWindow> {
	private static final long serialVersionUID = 4806387107507484829L;
	private transient Meter meter;
	private String metricName;

	public StationPlatformRichWindowFunction() {
		this.metricName = StationPlatformRichWindowFunction.class.getSimpleName();
	}

	public StationPlatformRichWindowFunction(String metricName) {
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
	public void apply(CompositeKeyStationPlatform key, TimeWindow window,
			Iterable<Tuple2<CompositeKeyStationPlatform, MqttSensor>> input, Collector<MqttSensor> out)
			throws Exception {
		this.meter.markEvent();

		// ProcessSomeStuff.processSomeStuff(key, 5);

		for (Tuple2<CompositeKeyStationPlatform, MqttSensor> tuple2 : input) {
			// CompositeKeyStationPlatform tupleKey = tuple2.f0;
			MqttSensor tupleValue = tuple2.f1;
			out.collect(tupleValue);
		}
	}
}

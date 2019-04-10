package org.sense.flink.examples.stream.udfs;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.sense.flink.mqtt.CompositeKeyStationPlatform;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.util.ProcessSomeStuff;

public class StationPlatformRichAllWindowFunction
		extends RichAllWindowFunction<Tuple2<CompositeKeyStationPlatform, MqttSensor>, MqttSensor, TimeWindow> {
	private static final long serialVersionUID = 895693565650147235L;

	private transient Meter meter;
	private String metricName;

	public StationPlatformRichAllWindowFunction() {
		this.metricName = StationPlatformRichAllWindowFunction.class.getSimpleName();
	}

	public StationPlatformRichAllWindowFunction(String metricName) {
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
	public void apply(TimeWindow window, Iterable<Tuple2<CompositeKeyStationPlatform, MqttSensor>> values,
			Collector<MqttSensor> out) throws Exception {
		this.meter.markEvent();
		// this.counter.inc();

		for (Tuple2<CompositeKeyStationPlatform, MqttSensor> tuple2 : values) {
			CompositeKeyStationPlatform tupleKey = tuple2.f0;
			MqttSensor tupleValue = tuple2.f1;
			ProcessSomeStuff.processSomeStuff(tupleKey, 5);
			out.collect(tupleValue);
		}
	}
}

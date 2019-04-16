package org.sense.flink.examples.stream.udfs;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.sense.flink.mqtt.CompositeKeySensorTypePlatformStation;
import org.sense.flink.mqtt.MqttSensor;

/**
 * This is just a UDF that takes metrics when we want to use ReduceFunction's.
 * RichReduceFunction's cannot be used with "reduce()" method. Therefore, we
 * need to use a ProcessWindowFunction. The "process()" method just collect all
 * data from the reduce UDF applied together with the reduce function.
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class MetricsProcessWindowFunction extends
		ProcessWindowFunction<Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>, Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>, CompositeKeySensorTypePlatformStation, TimeWindow> {
	private static final long serialVersionUID = 6630717069617679992L;

	// Create metrics
	// private transient Counter counter;
	private transient Meter meter;

	@Override
	public void open(Configuration config) throws Exception {
		// this.counter=getRuntimeContext().getMetricGroup().counter("counterSensorTypeMapper");
		com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
		this.meter = getRuntimeContext().getMetricGroup().meter(
				MetricsProcessWindowFunction.class.getSimpleName() + "-meter",
				new DropwizardMeterWrapper(dropwizardMeter));
	}

	@Override
	public void process(CompositeKeySensorTypePlatformStation arg0,
			ProcessWindowFunction<Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>, Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>, CompositeKeySensorTypePlatformStation, TimeWindow>.Context arg1,
			Iterable<Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>> records,
			Collector<Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>> out) throws Exception {
		this.meter.markEvent();
		// this.counter.inc();

		for (Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor> record : records) {
			out.collect(record);
		}
	}
}

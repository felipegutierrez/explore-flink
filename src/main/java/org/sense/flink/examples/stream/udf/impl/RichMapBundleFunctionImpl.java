package org.sense.flink.examples.stream.udf.impl;

import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.util.Collector;
import org.sense.flink.examples.stream.udf.RichMapBundleFunction;
import org.sense.flink.mqtt.CompositeKeySensorTypePlatformStation;
import org.sense.flink.mqtt.MqttSensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RichMapBundleFunctionImpl extends
		RichMapBundleFunction<CompositeKeySensorTypePlatformStation, MqttSensor, Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>, MqttSensor> {

	private static final long serialVersionUID = 6242924734451560830L;
	private static final Logger logger = LoggerFactory.getLogger(RichMapBundleFunctionImpl.class);

	// Create metrics
	private transient Counter counterIn;
	private transient Counter counterBundle;
	private transient Meter meterIn;
	private transient Meter meterBundle;
	private String metricName;

	public RichMapBundleFunctionImpl() {
		this.metricName = RichMapBundleFunctionImpl.class.getSimpleName();
	}

	public RichMapBundleFunctionImpl(String metricName) {
		this.metricName = metricName;
	}

	@Override
	public void open(Configuration config) throws Exception {
		this.counterIn = getRuntimeContext().getMetricGroup().counter(this.metricName + "-counterIn");
		this.counterBundle = getRuntimeContext().getMetricGroup().counter(this.metricName + "-counterBundle");

		com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
		this.meterIn = getRuntimeContext().getMetricGroup().meter(this.metricName + "-meterIn",
				new DropwizardMeterWrapper(dropwizardMeter));
		this.meterBundle = getRuntimeContext().getMetricGroup().meter(this.metricName + "-meterBundle",
				new DropwizardMeterWrapper(dropwizardMeter));
	}

	@Override
	public MqttSensor addInput(MqttSensor value, Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor> input) {
		this.meterIn.markEvent();
		this.counterIn.inc();

		if (value == null) {
			// System.out.println("addInput: input.f1: "+input.f1.getKey()+" |
			// "+input.f1.getTrip());
			return input.f1;
		} else {
			// System.out.println("addInput: value: "+value.getKey()+" | "+value.getTrip());
			// pre-aggregate values
			MqttSensor currentInput = input.f1;

			// check if keys are equal
			if (!currentInput.getKey().f1.equals(value.getKey().f1)
					|| !currentInput.getKey().f2.equals(value.getKey().f2)
					|| !currentInput.getKey().f4.equals(value.getKey().f4)) {
				logger.error("Keys are not equal [" + currentInput + "] - [" + value + "]");
			}

			Long timestamp = currentInput.getTimestamp() > value.getTimestamp() ? currentInput.getTimestamp()
					: value.getTimestamp();
			Double v = value.getValue() + currentInput.getValue();
			String trip = value.getTrip() + "|" + currentInput.getTrip();
			MqttSensor newValue = new MqttSensor(value.getTopic(), value.getKey(), timestamp, v, trip);
			// System.out.println("addInput: newValue: "+newValue.getKey()+" |
			// "+newValue.getTrip());
			return newValue;
		}
	}

	@Override
	public void finishBundle(Map<CompositeKeySensorTypePlatformStation, MqttSensor> buffer, Collector<MqttSensor> out)
			throws Exception {
		this.meterBundle.markEvent();
		this.counterBundle.inc();

		for (Map.Entry<CompositeKeySensorTypePlatformStation, MqttSensor> entry : buffer.entrySet()) {
			out.collect(entry.getValue());
			// System.out.println("finishBundle: "+entry.getValue().getKey()+" |
			// "+entry.getValue().getTrip());
		}
	}
}

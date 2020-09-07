package org.sense.flink.examples.stream.udf.impl;

import java.text.DecimalFormat;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.util.HyperLogLogState;

public class CompositeKeySensorTypePlatformStationKeyedProcessFunction
		extends KeyedProcessFunction<Byte, MqttSensor, String> {
	private static final long serialVersionUID = -1161858308397059065L;
	// delay after which an alert flag is thrown
	private final long timeOut;
	// state to remember the last timer set
	private transient ValueState<HyperLogLogState> hllState;
	private DecimalFormat dec = new DecimalFormat("#0.0000000000");

	public CompositeKeySensorTypePlatformStationKeyedProcessFunction(long timeOut) {
		this.timeOut = timeOut;
	}

	@Override
	public void open(Configuration conf) {
		// setup timer and HLL state
		ValueStateDescriptor<HyperLogLogState> descriptorHLL = new ValueStateDescriptor<>("hllState",
				HyperLogLogState.class);
		hllState = getRuntimeContext().getState(descriptorHLL);
	}

	@Override
	public void processElement(MqttSensor value, KeyedProcessFunction<Byte, MqttSensor, String>.Context ctx,
			Collector<String> out) throws Exception {
		// get current time and compute timeout time
		long currentTime = ctx.timerService().currentProcessingTime();
		long timeoutTime = currentTime + timeOut;

		// update HLL state
		HyperLogLogState currentHLLState = hllState.value();
		if (currentHLLState == null) {
			// System.out.println("process: " + currentTime + " - " + timeoutTime);
			currentHLLState = new HyperLogLogState();
			// register timer for timeout time
			ctx.timerService().registerProcessingTimeTimer(timeoutTime);
		}
		currentHLLState.offer(value);

		// remember timeout time
		currentHLLState.setLastTimer(timeoutTime);
		hllState.update(currentHLLState);
	}

	@Override
	public void onTimer(long timestamp, KeyedProcessFunction<Byte, MqttSensor, String>.OnTimerContext ctx,
			Collector<String> out) throws Exception {
		// check if this was the last timer we registered
		HyperLogLogState currentHLLState = hllState.value();
		// System.out.println("onTimer: " + timestamp + " - " +
		// currentHLLState.getLastTimer() + " = " + (currentHLLState.getLastTimer() -
		// timestamp));
		// it was, so no data was received afterwards. fire an alert.
		long estimative = currentHLLState.cardinality();
		int real = currentHLLState.getValues().size();
		// error (= [estimated cardinality - true cardinality] / true cardinality)
		double error = (double) (((double) (estimative - real)) / real);
		out.collect("Cardinality:\n estimate: " + estimative + " real: " + real + " error: " + dec.format(error) + "%");
		hllState.clear();
	}
}

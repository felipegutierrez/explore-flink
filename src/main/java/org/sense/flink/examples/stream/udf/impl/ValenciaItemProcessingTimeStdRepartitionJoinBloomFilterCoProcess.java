package org.sense.flink.examples.stream.udf.impl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.pojo.ValenciaItem;

/**
 * This is an implementation of a "Improved Repartition Join" algorithm
 * available on the paper "A Comparison of Join Algorithms for Log Processing in
 * MapReduce " using CoProcessFunction.
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaItemProcessingTimeStdRepartitionJoinBloomFilterCoProcess
		extends CoProcessFunction<ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>> {
	private static final long serialVersionUID = -5370773568848637379L;
	// 2019-07-22T12:51:04.681
	protected SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
	private ListState<ValenciaItem> trafficList = null;
	private ListState<ValenciaItem> pollutionList = null;
	private ValueState<Boolean> trigger = null;
	// delay after which an alert flag is thrown
	private final long timeOut;

	public ValenciaItemProcessingTimeStdRepartitionJoinBloomFilterCoProcess(long timeOut) {
		this.timeOut = timeOut;
	}

	@Override
	public void open(Configuration config) {
		// @formatter:off
		ListStateDescriptor<ValenciaItem> trafficDescriptor = new ListStateDescriptor<ValenciaItem>("trafficBuffer", ValenciaItem.class);
		trafficList = getRuntimeContext().getListState(trafficDescriptor);

		ListStateDescriptor<ValenciaItem> pollutionDescriptor = new ListStateDescriptor<ValenciaItem>("pollutionBuffer", ValenciaItem.class);
		pollutionList = getRuntimeContext().getListState(pollutionDescriptor);

		ValueStateDescriptor<Boolean> triggerDescriptor = new ValueStateDescriptor<Boolean>("triggerState", Boolean.class);
		trigger = getRuntimeContext().getState(triggerDescriptor);
		// @formatter:on
	}

	/**
	 * This method process only the Left source which is ValenciaTraffic.
	 */
	@Override
	public void processElement1(ValenciaItem traffic,
			CoProcessFunction<ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>>.Context context,
			Collector<Tuple2<ValenciaItem, ValenciaItem>> out) throws Exception {
		// get current time and compute timeout time
		long currentTime = context.timerService().currentProcessingTime();
		long timeoutTime = currentTime + timeOut;
		registerTimer(context, timeoutTime);

		traffic.clearCoordinates();
		// System.out.println("Received " + traffic);
		trafficList.add(traffic);
	}

	/**
	 * This method process only the Right source which is ValenciaPollution.
	 */
	@Override
	public void processElement2(ValenciaItem pollution,
			CoProcessFunction<ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>>.Context context,
			Collector<Tuple2<ValenciaItem, ValenciaItem>> out) throws Exception {
		// get current time and compute timeout time
		long currentTime = context.timerService().currentProcessingTime();
		long timeoutTime = currentTime + timeOut;
		registerTimer(context, timeoutTime);

		pollution.clearCoordinates();
		// System.out.println("Received " + pollution.toString());
		pollutionList.add(pollution);
	}

	private void registerTimer(
			CoProcessFunction<ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>>.Context context,
			long timeoutTime) throws IOException {
		if (trigger.value() == null) {
			trigger.update(true);
			context.timerService().registerProcessingTimeTimer(timeoutTime);
		}
	}

	@Override
	public void onTimer(long t, OnTimerContext context, Collector<Tuple2<ValenciaItem, ValenciaItem>> out)
			throws Exception {
		List<ValenciaItem> pollutionEntries = new ArrayList<ValenciaItem>();
		List<ValenciaItem> trafficEntries = new ArrayList<ValenciaItem>();
		synchronized (this) {
			pollutionList.get().iterator().forEachRemaining(pollutionEntries::add);
			pollutionList.clear();
			trafficList.get().iterator().forEachRemaining(trafficEntries::add);
			trafficList.clear();
			trigger.clear();
		}
		String msg = "Thread[" + Thread.currentThread().getId() + "] " + formatter.format(new Date(t));
		msg += " - pollution[" + pollutionEntries.size() + "] traffic[" + trafficEntries.size() + "]";
		int count = 0;

		String pollutionIds = "";
		// we can access only once the ValenciaPokkution item because it is the outer
		// loop on our nested loop.
		for (ValenciaItem pollution : pollutionEntries) {
			pollutionIds += pollution.getId() + "-";
			for (ValenciaItem traffic : trafficEntries) {
				if (pollution.getId().equals(traffic.getId())) {
					out.collect(Tuple2.of(pollution, traffic));
					count++;
				} else {
					System.out.println("different");
				}
			}
		}
		msg += " count[" + count + "] pollution ids[" + pollutionIds + "]";
		System.out.println(msg);
	}
}

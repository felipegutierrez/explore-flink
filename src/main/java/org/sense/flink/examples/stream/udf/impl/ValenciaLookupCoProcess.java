package org.sense.flink.examples.stream.udf.impl;

import java.io.IOException;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.util.ValenciaItemType;

import com.clearspring.analytics.stream.membership.BloomFilter;
import com.clearspring.analytics.stream.membership.Filter;

/**
 * The idea of this UDF is to send tuples that have a possibility to be in the
 * join operation on the following operator. We will send tuples only that
 * already match with items of the Side Output source.
 * 
 * @author Felipe Oliveira Gutierrez
 */
public class ValenciaLookupCoProcess
		extends CoProcessFunction<ValenciaItem, Tuple2<ValenciaItemType, Long>, ValenciaItem> {
	private static final long serialVersionUID = -5653918629637391518L;
	private Filter bloomFilterTrafficMatches;
	private Filter bloomFilterPollutionMatches;
	private Filter bloomFilterTrafficRedundant;
	private Filter bloomFilterPollutionRedundant;
	// delay after which an alert flag is thrown
	private final long timeOut;
	private ValueState<Boolean> trigger = null;

	public ValenciaLookupCoProcess(long timeOut) {
		this.timeOut = timeOut;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		initBloomFilters();

		ValueStateDescriptor<Boolean> triggerDescriptor = new ValueStateDescriptor<Boolean>("triggerState",
				Boolean.class);
		trigger = getRuntimeContext().getState(triggerDescriptor);
	}

	@Override
	public void onTimer(long timestamp,
			CoProcessFunction<ValenciaItem, Tuple2<ValenciaItemType, Long>, ValenciaItem>.OnTimerContext ctx,
			Collector<ValenciaItem> out) throws Exception {
		String msg = "Thread[" + Thread.currentThread().getId() + "] cleaning Bloom Filters";
		System.out.println(msg);
		initBloomFilters();
		trigger.clear();
	}

	/**
	 * The key is it will give false positive result, but never false negative. If
	 * the answer is TRUE it is not accurate. If the answer is FALSE it is within
	 * 100% accuracy.
	 */
	@Override
	public void processElement1(ValenciaItem valenciaItem,
			CoProcessFunction<ValenciaItem, Tuple2<ValenciaItemType, Long>, ValenciaItem>.Context context,
			Collector<ValenciaItem> out) throws Exception {
		registerTimeout(context);

		String key = valenciaItem.getId().toString();
		if (valenciaItem.getType() == ValenciaItemType.TRAFFIC_JAM) {
			// If the key is not redundant and if it is likely to match
			if (!bloomFilterTrafficRedundant.isPresent(key) && bloomFilterPollutionMatches.isPresent(key)) {
				out.collect(valenciaItem);
				bloomFilterTrafficRedundant.add(key);
			}
		} else if (valenciaItem.getType() == ValenciaItemType.AIR_POLLUTION) {
			// If the key is not redundant and if it is likely to match
			if (!bloomFilterPollutionRedundant.isPresent(key) && bloomFilterTrafficMatches.isPresent(key)) {
				out.collect(valenciaItem);
				bloomFilterPollutionRedundant.add(key);
			}
		} else if (valenciaItem.getType() == ValenciaItemType.NOISE) {
		} else {
			throw new Exception("ValenciaItemType is NULL!");
		}
	}

	@Override
	public void processElement2(Tuple2<ValenciaItemType, Long> lookupValue,
			CoProcessFunction<ValenciaItem, Tuple2<ValenciaItemType, Long>, ValenciaItem>.Context context,
			Collector<ValenciaItem> out) throws Exception {
		registerTimeout(context);

		String key = lookupValue.f1.toString();
		if (lookupValue.f0 == ValenciaItemType.TRAFFIC_JAM) {
			bloomFilterTrafficMatches.add(key);
		} else if (lookupValue.f0 == ValenciaItemType.AIR_POLLUTION) {
			bloomFilterPollutionMatches.add(key);
		} else if (lookupValue.f0 == ValenciaItemType.NOISE) {
		} else {
			throw new Exception("ValenciaItemType is NULL!");
		}
	}

	/**
	 * Get current time and compute timeout time.
	 * 
	 * @param context
	 * @throws IOException
	 */
	private void registerTimeout(
			CoProcessFunction<ValenciaItem, Tuple2<ValenciaItemType, Long>, ValenciaItem>.Context context)
			throws IOException {
		if (trigger.value() == null) {
			trigger.update(true);
			context.timerService()
					.registerProcessingTimeTimer(context.timerService().currentProcessingTime() + timeOut);
		}
	}

	private void initBloomFilters() {
		bloomFilterTrafficMatches = new BloomFilter(100, 0.01);
		bloomFilterPollutionMatches = new BloomFilter(100, 0.01);
		bloomFilterTrafficRedundant = new BloomFilter(100, 0.01);
		bloomFilterPollutionRedundant = new BloomFilter(100, 0.01);
	}
}

package org.sense.flink.examples.stream.udf.impl;

import java.text.SimpleDateFormat;
import java.util.Date;

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
 * https://github.com/ververica/flink-training-exercises/blob/master/src/main/java/com/dataartisans/flinktraining/examples/datastream_java/process/CarEventSort.java#L79
 * https://github.com/ververica/flink-training-exercises/blob/master/src/main/java/com/dataartisans/flinktraining/exercises/datastream_java/utils/ConnectedCarAssigner.java#L23
 * 
 * 
 * @author Felipe Oliveira Gutierrez
 */
public class ValenciaLookupCoProcess
		extends CoProcessFunction<ValenciaItem, Tuple2<ValenciaItemType, Long>, ValenciaItem> {
	private static final long serialVersionUID = -5653918629637391518L;
	private final SimpleDateFormat sdf;
	private Filter bloomFilterTrafficMatches;
	private Filter bloomFilterPollutionMatches;
	private Filter bloomFilterTrafficRedundant;
	private Filter bloomFilterPollutionRedundant;
	private final long dataSourceFrequency;
	private final long watermarkFrequency;
	private ValueState<String> state;

	public ValenciaLookupCoProcess(long dataSourceFrequency, long watermarkFrequency) {
		this.sdf = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
		this.dataSourceFrequency = dataSourceFrequency;
		this.watermarkFrequency = watermarkFrequency;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", String.class));
		initBloomFilters();
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
		long valenciaTimestamp = valenciaItem.getTimestamp();
		long watermark = context.timerService().currentWatermark();
		boolean flag = watermark > 0 && (valenciaTimestamp - watermark - watermarkFrequency) < dataSourceFrequency;

		String msg = "[" + Thread.currentThread().getId() + " " + valenciaItem.getType() + "] ";
		msg += "ts[" + sdf.format(new Date(valenciaTimestamp)) + "] ";
		msg += "W[" + sdf.format(new Date(watermark)) + "] ";
		msg += "[" + (valenciaTimestamp - watermark - watermarkFrequency) + " " + flag + "] ";
		// System.out.println(msg);
		if (flag) {
			state.update(valenciaItem.getType().toString());
			// schedule the next timer 60 seconds from the current event time
			context.timerService().registerEventTimeTimer(watermark + watermarkFrequency);
		}

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

	@Override
	public void onTimer(long timestamp,
			CoProcessFunction<ValenciaItem, Tuple2<ValenciaItemType, Long>, ValenciaItem>.OnTimerContext context,
			Collector<ValenciaItem> out) throws Exception {
		long watermark = context.timerService().currentWatermark();
		String msg = "[" + Thread.currentThread().getId() + "] onTimer(" + state.value() + ") ";
		msg += "ts[" + sdf.format(new Date(timestamp)) + "] ";
		msg += "W[" + sdf.format(new Date(watermark)) + "] ";
		msg += "ts[" + timestamp + "] ";
		msg += "W[" + watermark + "] ";
		System.out.println(msg);
		if (watermark >= timestamp) {
			initBloomFilters();
		}
	}

	private void initBloomFilters() {
		bloomFilterTrafficMatches = new BloomFilter(100, 0.01);
		bloomFilterPollutionMatches = new BloomFilter(100, 0.01);
		bloomFilterTrafficRedundant = new BloomFilter(100, 0.01);
		bloomFilterPollutionRedundant = new BloomFilter(100, 0.01);
	}
}

package org.sense.flink.examples.stream.udf.impl;

import java.text.SimpleDateFormat;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.pojo.ValenciaItem;

public class ValenciaItemProcessingTimeBroadcastJoinKeyedBroadcastProcess
		extends KeyedBroadcastProcessFunction<Long, ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>> {
	private static final long serialVersionUID = 4222786536907971725L;
	// 2019-07-22T12:51:04.681
	protected SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");

	// identical to our PollutionBroadcastState above
	private MapStateDescriptor<Long, ValenciaItem> bcStateDescriptor;
	// private ListState<ValenciaItem> trafficList = null;
	private ValueState<Boolean> trigger = null;
	// delay after which an alert flag is thrown
	private final long timeOut;

	public ValenciaItemProcessingTimeBroadcastJoinKeyedBroadcastProcess(long timeOut) {
		this.timeOut = timeOut;
	}

	@Override
	public void open(Configuration conf) {
		// @formatter:off
		// ListStateDescriptor<ValenciaItem> trafficDescriptor = new ListStateDescriptor<ValenciaItem>("trafficBuffer", ValenciaItem.class);
		// trafficList = getRuntimeContext().getListState(trafficDescriptor);
		
		bcStateDescriptor = new MapStateDescriptor<Long, ValenciaItem>("PollutionBroadcastState", Types.LONG, TypeInformation.of(new TypeHint<ValenciaItem>() {}));
		
		ValueStateDescriptor<Boolean> triggerDescriptor = new ValueStateDescriptor<Boolean>("triggerState", Boolean.class);
		trigger = getRuntimeContext().getState(triggerDescriptor);
		// @formatter:on
	}

	/**
	 * Here we process the Pollution ValenciaItem. This method is called for each
	 * new pollution item and it overwrites the current pollution item regarding the
	 * districtID with the new pollution item.
	 */
	@Override
	public void processBroadcastElement(ValenciaItem pollution,
			KeyedBroadcastProcessFunction<Long, ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>>.Context ctx,
			Collector<Tuple2<ValenciaItem, ValenciaItem>> out) throws Exception {
		BroadcastState<Long, ValenciaItem> bcState = ctx.getBroadcastState(bcStateDescriptor);
		bcState.put(pollution.getId(), pollution);
		String msg = "Thread[" + Thread.currentThread().getId() + "] ";// + formatter.format(new Date(t));
		msg += " - pollution[" + pollution.getId() + "]";
		System.out.println(msg);
	}

	/**
	 * This is the ReadOnlyContext side of the join.
	 */
	@Override
	public void processElement(ValenciaItem traffic,
			KeyedBroadcastProcessFunction<Long, ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>>.ReadOnlyContext ctx,
			Collector<Tuple2<ValenciaItem, ValenciaItem>> out) throws Exception {
		// get current time and compute timeout time
		long currentTime = ctx.timerService().currentProcessingTime();
		long timeoutTime = currentTime + timeOut;
		if (trigger.value() == null) {
			trigger.update(true);
			ctx.timerService().registerProcessingTimeTimer(timeoutTime);
		}

		// get current pollution item from broadcast state which has the same trafficID
		ReadOnlyBroadcastState<Long, ValenciaItem> bcState = ctx.getBroadcastState(bcStateDescriptor);
		ValenciaItem pollutionItem = bcState.get(traffic.getId());

		String msg = "Thread[" + Thread.currentThread().getId() + "] ";// + formatter.format(new Date(t));
		msg += " - traffic[" + traffic.getId() + "]";

		if (pollutionItem != null) {
			// System.out.println(msg);
			out.collect(Tuple2.of(traffic, pollutionItem));
		} else {
			msg += " NOT FOUND.";
			// System.out.println(msg);
		}
	}

	@Override
	public void onTimer(long timestamp,
			KeyedBroadcastProcessFunction<Long, ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>>.OnTimerContext ctx,
			Collector<Tuple2<ValenciaItem, ValenciaItem>> out) throws Exception {
		ReadOnlyBroadcastState<Long, ValenciaItem> bcState = ctx.getBroadcastState(bcStateDescriptor);
		bcState.clear();
		trigger.clear();
	}
}

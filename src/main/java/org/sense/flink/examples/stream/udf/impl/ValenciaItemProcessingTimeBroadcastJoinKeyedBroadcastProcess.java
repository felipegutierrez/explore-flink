package org.sense.flink.examples.stream.udf.impl;

import java.util.Map;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.pojo.ValenciaItem;

public class ValenciaItemProcessingTimeBroadcastJoinKeyedBroadcastProcess
		extends KeyedBroadcastProcessFunction<Long, ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>> {
	private static final long serialVersionUID = 4222786536907971725L;
	// identical to our PollutionBroadcastState above
	private MapStateDescriptor<Long, ValenciaItem> pollutionStateDescriptor = null;
	private ListStateDescriptor<ValenciaItem> trafficDescriptor = null;
	// delay after which an alert flag is thrown
	private final long timeOut;

	@Override
	public void open(Configuration parameters) throws Exception {
		pollutionStateDescriptor = new MapStateDescriptor<Long, ValenciaItem>("PollutionBroadcastState",
				BasicTypeInfo.LONG_TYPE_INFO, TypeInformation.of(new TypeHint<ValenciaItem>() {
				}));

		trafficDescriptor = new ListStateDescriptor<ValenciaItem>("trafficBuffer", ValenciaItem.class);
	}

	public ValenciaItemProcessingTimeBroadcastJoinKeyedBroadcastProcess(long timeOut) {
		this.timeOut = timeOut;
	}

	/**
	 * Here we process the Pollution ValenciaItem
	 */
	@Override
	public void processBroadcastElement(ValenciaItem pollution,
			KeyedBroadcastProcessFunction<Long, ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>>.Context ctx,
			Collector<Tuple2<ValenciaItem, ValenciaItem>> out) throws Exception {
		ctx.getBroadcastState(pollutionStateDescriptor).put(pollution.getId(), pollution);
	}

	/**
	 * This is the ReadOnlyContext side of the join.
	 */
	@Override
	public void processElement(ValenciaItem traffic,
			KeyedBroadcastProcessFunction<Long, ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>>.ReadOnlyContext ctx,
			Collector<Tuple2<ValenciaItem, ValenciaItem>> out) throws Exception {
		final ListState<ValenciaItem> state = getRuntimeContext().getListState(trafficDescriptor);

		for (Map.Entry<Long, ValenciaItem> pollutionItem : ctx.getBroadcastState(pollutionStateDescriptor)
				.immutableEntries()) {
			final Long pollutionDistrictId = pollutionItem.getKey();
			final ValenciaItem pollution = pollutionItem.getValue();
		}
	}
}

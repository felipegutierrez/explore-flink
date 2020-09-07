package org.sense.flink.examples.stream.udf.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.pojo.ValenciaItem;

public class ValenciaItemEventTimeJoinCoProcess
		extends CoProcessFunction<ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>> {
	private static final long serialVersionUID = -313141769551931274L;
	// Store pending Traffic and Pollution for a districtId, keyed by timestamp
	private MapState<Long, ValenciaItem> trafficMap = null;
	private MapState<Long, ValenciaItem> pollutionMap = null;

	@Override
	public void open(Configuration config) {
		MapStateDescriptor<Long, ValenciaItem> trafficDescriptor = new MapStateDescriptor<Long, ValenciaItem>(
				"trafficBuffer", TypeInformation.of(Long.class), TypeInformation.of(ValenciaItem.class));
		trafficMap = getRuntimeContext().getMapState(trafficDescriptor);

		MapStateDescriptor<Long, ValenciaItem> pollutionDescriptor = new MapStateDescriptor<Long, ValenciaItem>(
				"pollutionBuffer", TypeInformation.of(Long.class), TypeInformation.of(ValenciaItem.class));
		pollutionMap = getRuntimeContext().getMapState(pollutionDescriptor);
	}

	/**
	 * This method process only the Left source which is ValenciaTraffic.
	 */
	@Override
	public void processElement1(ValenciaItem traffic,
			CoProcessFunction<ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>>.Context context,
			Collector<Tuple2<ValenciaItem, ValenciaItem>> out) throws Exception {
		// System.out.println("Received " + traffic);
		TimerService timerService = context.timerService();

		if (context.timestamp() > timerService.currentWatermark()) {
			// Do the join later, by which time any relevant Pollution records should have
			// arrived
			trafficMap.put(traffic.getTimestamp(), traffic);
			timerService.registerEventTimeTimer(traffic.getTimestamp());
		} else {
			// Late Traffic records land here.
			System.out.println("Late traffic record");
		}
	}

	/**
	 * This method process only the Right source which is ValenciaPollution.
	 */
	@Override
	public void processElement2(ValenciaItem pollution,
			CoProcessFunction<ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>>.Context context,
			Collector<Tuple2<ValenciaItem, ValenciaItem>> out) throws Exception {
		// System.out.println("Received " + pollution.toString());
		pollutionMap.put(pollution.getTimestamp(), pollution);

		/*
		 * Calling this solely for its side effect of freeing older ValenciaPollution
		 * records. Otherwise ValenciaPollution with frequent updates and no
		 * ValenciaTraffic would leak state.
		 */
		getPollutionRecordToJoin(context.timerService().currentWatermark());
	}

	@Override
	public void onTimer(long t, OnTimerContext context, Collector<Tuple2<ValenciaItem, ValenciaItem>> out)
			throws Exception {
		ValenciaItem traffic = trafficMap.get(t);
		if (traffic != null) {
			trafficMap.remove(t);

			ValenciaItem pollution = getPollutionRecordToJoin(traffic.getTimestamp());
			String value = "Traffic [" + traffic.getValue() + "] - " + pollution.getValue();

			out.collect(Tuple2.of(pollution, traffic));
		}
	}

	/*
	 * Returns the newest Pollution that isn't newer than the Traffic we are
	 * enriching. As a side effect, removes earlier Pollution records that are no
	 * longer needed.
	 */
	private ValenciaItem getPollutionRecordToJoin(Long timestamp) throws Exception {
		Iterator<Map.Entry<Long, ValenciaItem>> pollutionEntries = pollutionMap.iterator();
		ValenciaItem theOneWeAreLookingFor = null;
		List<Long> toRemove = new ArrayList<Long>();

		while (pollutionEntries.hasNext()) {
			ValenciaItem pollutionItem = pollutionEntries.next().getValue();
			// pollutionItem should not be newer than the ValenciaTraffic being enriched
			if (pollutionItem.getTimestamp() <= timestamp) {
				/*
				 * By the time ValenciaTraffic are being joined, they are being processed in
				 * order (by Timestamp). This means that any ValenciaPollution record too old to
				 * join with this ValenciaTraffic is too old to be worth keeping any longer.
				 */
				if (theOneWeAreLookingFor != null) {
					if (pollutionItem.getTimestamp() > theOneWeAreLookingFor.getTimestamp()) {
						toRemove.add(theOneWeAreLookingFor.getTimestamp());
						theOneWeAreLookingFor = pollutionItem;
					}
				} else {
					theOneWeAreLookingFor = pollutionItem;
				}
			}
		}
		for (Long t : toRemove) {
			System.out.println("Removing pollution @ " + t);
			pollutionMap.remove(t);
		}
		return theOneWeAreLookingFor;
	}
}

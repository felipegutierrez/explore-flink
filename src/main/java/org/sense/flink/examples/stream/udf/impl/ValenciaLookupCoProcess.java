package org.sense.flink.examples.stream.udf.impl;

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
import org.sense.flink.util.ValenciaBloomFilterState;
import org.sense.flink.util.ValenciaItemType;

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
	private ValueState<ValenciaBloomFilterState> bloomFilter;
	private ListState<ValenciaItem> valenciaItemLeftTable;
	private final long timeOut;
	private final boolean distinct;
	private final boolean lookup;

	/**
	 * If @distinct is TRUE it will preserve only one (the first) item for distinct
	 * keys. If @distinct is FALSE it will combine all items of the same key in only
	 * one item. For the second option it is necessary a different state.
	 * 
	 * @param timeOut
	 */
	public ValenciaLookupCoProcess(long timeOut) {
		this(timeOut, true, true);
	}

	public ValenciaLookupCoProcess(long timeOut, boolean distinct) {
		this(timeOut, distinct, true);
	}

	public ValenciaLookupCoProcess(long timeOut, boolean distinct, boolean lookup) {
		this.sdf = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss.SSS");
		this.timeOut = timeOut;
		this.distinct = distinct;
		this.lookup = lookup;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		ValueStateDescriptor<ValenciaBloomFilterState> bloomFilterDesc = new ValueStateDescriptor<ValenciaBloomFilterState>(
				"bloomFilter", ValenciaBloomFilterState.class);
		bloomFilter = getRuntimeContext().getState(bloomFilterDesc);

		ListStateDescriptor<ValenciaItem> listStateProperties = new ListStateDescriptor<ValenciaItem>(
				"ValenciaItemListState", ValenciaItem.class);
		valenciaItemLeftTable = getRuntimeContext().getListState(listStateProperties);
	}

	/**
	 * The key is it will give false positive result, but never false negative. If
	 * the answer is TRUE it is not accurate. If the answer is FALSE it is within
	 * 100% accuracy. Hence, deduplication checks will be 100% accurate.
	 * 
	 * This side is considered the LEFT side of the JOIN. hence, the keys of this
	 * side are added on the LEFT sub-state. However we check the RIGHT sub-state if
	 * it ill match in the future.
	 */
	@Override
	public void processElement1(ValenciaItem valenciaItem,
			CoProcessFunction<ValenciaItem, Tuple2<ValenciaItemType, Long>, ValenciaItem>.Context context,
			Collector<ValenciaItem> out) throws Exception {
		ValenciaBloomFilterState state = bloomFilter.value();
		boolean flagUpdateState = false;

		if (state == null) {
			state = new ValenciaBloomFilterState();
			state.setLastModified(context.timestamp());
			// get current time and compute timeout time
			context.timerService().registerEventTimeTimer(state.getLastModified() + timeOut);
			flagUpdateState = true;

			// debug
			// if (valenciaItem.getType() == ValenciaItemType.TRAFFIC_JAM) {
			// String msg = "[" + Thread.currentThread().getId() + " " +
			// valenciaItem.getType() + "] ";
			// msg += "cts[" + sdf.format(new Date(state.getLastModified())) + "] ";
			// msg += "f[" + sdf.format(new Date(state.getLastModified() + timeOut)) + "] ";
			// System.out.println(msg);
			// }
		}

		if (distinct) {
			String key = valenciaItem.getId().toString();
			String hash = getHashCode(valenciaItem);
			if (valenciaItem.getType() == ValenciaItemType.TRAFFIC_JAM) {
				// If the key is not redundant
				if (!state.isPresentLeft(hash)) {
					if (lookup) {
						// if it is likely to match with the RIGHT table
						valenciaItemLeftTable.add(valenciaItem);
					} else {
						out.collect(valenciaItem);
					}
					state.addLeft(hash);
					flagUpdateState = true;
				}
			} else if (valenciaItem.getType() == ValenciaItemType.AIR_POLLUTION) {
				// If the key is not redundant
				if (!state.isPresentLeft(hash)) {
					if (lookup) {
						// if it is likely to match with the RIGHT table
						valenciaItemLeftTable.add(valenciaItem);
					} else {
						out.collect(valenciaItem);
					}
					state.addLeft(hash);
					flagUpdateState = true;
				}
			} else if (valenciaItem.getType() == ValenciaItemType.NOISE) {
			} else {
				throw new Exception("ValenciaItemType is NULL!");
			}
			if (flagUpdateState) {
				bloomFilter.update(state);
			}
		} else {
			System.err.println("TODO: Combine by key");
		}
	}

	/**
	 * This is considered the RIGHT side of the Join. Hence, the states on this side
	 * add keys on the Right sub-state.
	 */
	@Override
	public void processElement2(Tuple2<ValenciaItemType, Long> lookupValue,
			CoProcessFunction<ValenciaItem, Tuple2<ValenciaItemType, Long>, ValenciaItem>.Context context,
			Collector<ValenciaItem> out) throws Exception {
		boolean flagUpdateState = false;
		ValenciaBloomFilterState state = bloomFilter.value();
		if (state == null) {
			state = new ValenciaBloomFilterState();
			state.setLastModified(context.timestamp());
			context.timerService().registerEventTimeTimer(state.getLastModified() + timeOut);
			flagUpdateState = true;
		}
		if (lookup) {
			String key = lookupValue.f1.toString();
			if (lookupValue.f0 == ValenciaItemType.TRAFFIC_JAM) {
				state.addRight(key);
				flagUpdateState = true;
			} else if (lookupValue.f0 == ValenciaItemType.AIR_POLLUTION) {
				state.addRight(key);
				flagUpdateState = true;
			} else if (lookupValue.f0 == ValenciaItemType.NOISE) {
			} else {
				throw new Exception("ValenciaItemType is NULL!");
			}
		}
		if (flagUpdateState) {
			bloomFilter.update(state);
		}
	}

	@Override
	public void onTimer(long timestamp,
			CoProcessFunction<ValenciaItem, Tuple2<ValenciaItemType, Long>, ValenciaItem>.OnTimerContext context,
			Collector<ValenciaItem> out) throws Exception {
		ValenciaBloomFilterState state = bloomFilter.value();
		String msg = "[" + Thread.currentThread().getId() + " onTimer()] ";
		msg += "t[" + sdf.format(new Date(state.getLastModified() + timeOut)) + "] ";
		msg += "ts[" + sdf.format(new Date(timestamp)) + "] ";

		if (state != null && timestamp == state.getLastModified() + timeOut) {
			msg += "TRUE";
			bloomFilter.update(null);
		}
		// debug
		System.out.println(msg);

		List<ValenciaItem> valenciaItemLeftList = new ArrayList<ValenciaItem>();
		valenciaItemLeftTable.get().iterator().forEachRemaining(valenciaItemLeftList::add);
		valenciaItemLeftTable.clear();
		for (ValenciaItem valenciaItem : valenciaItemLeftList) {
			String key = valenciaItem.getId().toString();
			if (state.isPresentRight(key)) {
				out.collect(valenciaItem);
			}
		}
	}

	private String getHashCode(ValenciaItem valenciaItem) {
		Long key = valenciaItem.getId();
		Object value = valenciaItem.getValue();
		if (key != null && value != null) {
			return String.valueOf(key.hashCode()) + String.valueOf(value.hashCode());
		} else if (key != null && value == null) {
			return String.valueOf(key.hashCode());
		}
		return "";
	}
}

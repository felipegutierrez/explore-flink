package org.sense.flink.examples.stream.udf.impl;

import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.sense.flink.examples.stream.udf.MapBundleFunction;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.util.ValenciaItemType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapBundleValenciaImpl
		extends MapBundleFunction<Long, ValenciaItem, Tuple2<Long, ValenciaItem>, Tuple2<Long, ValenciaItem>> {
	private static final long serialVersionUID = -4457463922328881937L;
	private static final Logger logger = LoggerFactory.getLogger(MapBundleValenciaImpl.class);

	@Override
	public ValenciaItem addInput(ValenciaItem value, Tuple2<Long, ValenciaItem> input) throws Exception {
		if (value == null) {
			// System.out.println("addInput: input.f1: "+input.f1.getKey()+" |
			// "+input.f1.getTrip());
			return input.f1;
		} else {
			// System.out.println("addInput: " + value.getId() + " | " + value.getType() + "
			// | " + value.getValue());
			// pre-aggregate values
			ValenciaItem newValue = input.f1;

			// check if keys are equal
			// if (!newValue.getId().equals(value.getId()) || newValue.getType() !=
			// value.getType()) {
			// logger.info("Keys are not equal [" + newValue + "] - [" + value + "]");
			// } else
			if (newValue.getId().equals(value.getId()) && newValue.getType() == value.getType()) {
				if (newValue.getType() == ValenciaItemType.TRAFFIC_JAM) {
					newValue.addValue(value.getValue());
				} else if (newValue.getType() == ValenciaItemType.AIR_POLLUTION) {
					newValue.addValue(value.getValue());
				} else if (newValue.getType() == ValenciaItemType.NOISE) {
					throw new Exception("ValenciaItemType NOISE is not implemented!");
				} else {
					throw new Exception("ValenciaItemType is NULL!");
				}
			}
			return newValue;
		}
	}

	@Override
	public void finishBundle(Map<Long, ValenciaItem> buffer, Collector<Tuple2<Long, ValenciaItem>> out)
			throws Exception {
		for (Map.Entry<Long, ValenciaItem> entry : buffer.entrySet()) {
			out.collect(Tuple2.of(entry.getKey(), entry.getValue()));
		}
	}
}

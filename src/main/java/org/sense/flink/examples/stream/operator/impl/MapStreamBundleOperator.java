package org.sense.flink.examples.stream.operator.impl;

import org.apache.flink.api.java.functions.KeySelector;
import org.sense.flink.examples.stream.operator.AbstractMapStreamBundleOperator;
import org.sense.flink.examples.stream.trigger.BundleTrigger;
import org.sense.flink.examples.stream.udf.MapBundleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapStreamBundleOperator<K, V, IN, OUT> extends AbstractMapStreamBundleOperator<K, V, IN, OUT> {

	private static final Logger logger = LoggerFactory.getLogger(MapStreamBundleOperator.class);
	private static final long serialVersionUID = 6556268125924098320L;

	/** KeySelector is used to extract key for bundle map. */
	private final KeySelector<IN, K> keySelector;

	public MapStreamBundleOperator(MapBundleFunction<K, V, IN, OUT> function, BundleTrigger<IN> bundleTrigger,
			KeySelector<IN, K> keySelector) {
		super(function, bundleTrigger);
		this.keySelector = keySelector;
	}

	@Override
	protected K getKey(IN input) throws Exception {
		return this.keySelector.getKey(input);
	}
}

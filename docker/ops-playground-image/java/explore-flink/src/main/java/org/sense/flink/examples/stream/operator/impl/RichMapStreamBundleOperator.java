package org.sense.flink.examples.stream.operator.impl;

import org.apache.flink.api.java.functions.KeySelector;
import org.sense.flink.examples.stream.operator.AbstractRichMapStreamBundleOperator;
import org.sense.flink.examples.stream.trigger.BundleTrigger;
import org.sense.flink.examples.stream.udf.RichMapBundleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RichMapStreamBundleOperator<K, V, IN, OUT> extends AbstractRichMapStreamBundleOperator<K, V, IN, OUT> {

	private static final long serialVersionUID = 6605602698853987298L;
	private static final Logger logger = LoggerFactory.getLogger(RichMapStreamBundleOperator.class);

	/** KeySelector is used to extract key for bundle map. */
	private final KeySelector<IN, K> keySelector;

	public RichMapStreamBundleOperator(RichMapBundleFunction<K, V, IN, OUT> function, BundleTrigger<IN> bundleTrigger,
			KeySelector<IN, K> keySelector) {
		super(function, bundleTrigger);
		this.keySelector = keySelector;
	}

	@Override
	protected K getKey(IN input) throws Exception {
		return this.keySelector.getKey(input);
	}
}

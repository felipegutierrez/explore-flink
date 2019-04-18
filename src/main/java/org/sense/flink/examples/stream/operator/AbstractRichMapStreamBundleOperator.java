package org.sense.flink.examples.stream.operator;

import static org.apache.flink.util.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.sense.flink.examples.stream.trigger.BundleTrigger;
import org.sense.flink.examples.stream.trigger.BundleTriggerCallback;
import org.sense.flink.examples.stream.udf.RichMapBundleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link StreamOperator} for executing {@link RichMapBundleFunction
 * MapFunctions}.
 */
public abstract class AbstractRichMapStreamBundleOperator<K, V, IN, OUT>
		extends AbstractUdfStreamOperator<OUT, RichMapBundleFunction<K, V, IN, OUT>>
		implements OneInputStreamOperator<IN, OUT>, BundleTriggerCallback {

	private static final Logger logger = LoggerFactory.getLogger(AbstractRichMapStreamBundleOperator.class);
	private static final long serialVersionUID = 1L;

	/** The map in heap to store elements. */
	private final Map<K, V> bundle;

	/**
	 * The trigger that determines how many elements should be put into a bundle.
	 */
	private final BundleTrigger<IN> bundleTrigger;

	/** Output for stream records. */
	private transient TimestampedCollector<OUT> collector;

	private transient int numOfElements = 0;

	public AbstractRichMapStreamBundleOperator(RichMapBundleFunction<K, V, IN, OUT> function,
			BundleTrigger<IN> bundleTrigger) {
		super(function);
		chainingStrategy = ChainingStrategy.ALWAYS;
		this.bundle = new HashMap<>();
		this.bundleTrigger = checkNotNull(bundleTrigger, "bundleTrigger is null");
	}

	@Override
	public void open() throws Exception {
		super.open();

		numOfElements = 0;
		collector = new TimestampedCollector<>(output);

		bundleTrigger.registerCallback(this);
		// reset trigger
		bundleTrigger.reset();
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		// get the key and value for the map bundle
		final IN input = element.getValue();
		final K bundleKey = getKey(input);
		final V bundleValue = this.bundle.get(bundleKey);

		// get a new value after adding this element to bundle
		final V newBundleValue = userFunction.addInput(bundleValue, input);

		// update to map bundle
		bundle.put(bundleKey, newBundleValue);

		numOfElements++;
		bundleTrigger.onElement(input);
	}

	/**
	 * Get the key for current processing element, which will be used as the map
	 * bundle's key.
	 */
	protected abstract K getKey(final IN input) throws Exception;

	@Override
	public void finishBundle() throws Exception {
		if (!bundle.isEmpty()) {
			numOfElements = 0;
			userFunction.finishBundle(bundle, collector);
			bundle.clear();
		}
		bundleTrigger.reset();
	}
}

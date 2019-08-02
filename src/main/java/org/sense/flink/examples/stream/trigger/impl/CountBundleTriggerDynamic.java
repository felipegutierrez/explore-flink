package org.sense.flink.examples.stream.trigger.impl;

import java.text.DecimalFormat;

import org.apache.flink.util.Preconditions;
import org.sense.flink.examples.stream.trigger.BundleTrigger;
import org.sense.flink.examples.stream.trigger.BundleTriggerCallback;
import org.sense.flink.examples.stream.trigger.BundleTriggerDynamic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.ICardinality;

/**
 * A {@link BundleTrigger} that fires once the count of elements in a bundle
 * reaches the given count.
 */
public class CountBundleTriggerDynamic<K, T> implements BundleTriggerDynamic<K, T> {
	private static final Logger logger = LoggerFactory.getLogger(CountBundleTriggerDynamic.class);
	private static final long serialVersionUID = 8806579977649767427L;

	private DecimalFormat dec = new DecimalFormat("#0.0000000000");
	private final long LIMIT_MAX_COUNT = 1000;
	private final long LIMIT_MIN_COUNT = 1;
	private final long INCREMENT = 5;

	private long maxCount;
	private transient long count = 0;
	private transient BundleTriggerCallback callback;
	private transient ICardinality cardinality;

	public CountBundleTriggerDynamic() throws Exception {
		initCardinalitySketch();
		this.maxCount = LIMIT_MIN_COUNT;
		Preconditions.checkArgument(this.maxCount > 0, "maxCount must be greater than 0");
	}

	@Override
	public void registerCallback(BundleTriggerCallback callback) {
		this.callback = Preconditions.checkNotNull(callback, "callback is null");
		initCardinalitySketch();
	}

	@Override
	public void onElement(K key, T element) throws Exception {
		// add key element on the HyperLogLog to infer the data-stream cardinality
		this.cardinality.offer(key);
		// System.out.println(key);
		count++;
		if (count >= maxCount) {
			callback.finishBundle();
			reset();
		}
	}

	@Override
	public void reset() {
		if (count != 0) {
			long cardinalityHLL = this.cardinality.cardinality();
			double selectivity = Double.parseDouble(String.valueOf(cardinalityHLL))
					/ Double.parseDouble(String.valueOf(count));
			double itemsPercentToShuffle = (selectivity / cardinalityHLL);

			String msg = "cardinalityHLL[" + cardinalityHLL + "] count[" + count + "] selectivity["
					+ dec.format(selectivity) + "]";
			logger.info(msg);
			System.out.println(msg);

			/**
			 * <pre>
			 * If errorDegree is 1 it means we don't have the COmbiner working.
			 * If (errorDegree * 100) > 0.9 it means more than 90% of the items are useless during the shuffle process
			 * If (errorDegree * 100) < 0.5 it means less then 50% of the items are going to the shuffle phase.
			 * </pre>
			 */
			if (itemsPercentToShuffle == 1.0 || (itemsPercentToShuffle * 100) > 0.9) {
				// necessary to apply combiner
				if (maxCount + INCREMENT < LIMIT_MAX_COUNT) {
					maxCount = maxCount + INCREMENT;
				}
			} else if ((itemsPercentToShuffle * 100) < 0.5) {
				// reduce the combiner degree
				if (maxCount - INCREMENT > LIMIT_MIN_COUNT) {
					maxCount = maxCount - INCREMENT;
				}
			}
		}
		count = 0;
		cardinality = new HyperLogLog(16);
	}

	private void initCardinalitySketch() {
		if (this.cardinality == null) {
			this.cardinality = new HyperLogLog(16);
		}
	}

	@Override
	public String explain() {
		return "CountBundleTrigger with size " + maxCount;
	}

}

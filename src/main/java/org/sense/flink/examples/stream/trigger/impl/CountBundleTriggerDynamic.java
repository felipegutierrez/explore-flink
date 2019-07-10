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

	private long maxCount;
	private long increment = 100;
	private transient long count = 0;
	private transient BundleTriggerCallback callback;
	private transient ICardinality cardinality;

	public CountBundleTriggerDynamic() throws Exception {
		initCardinalitySketch();
		this.maxCount = increment;
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
		// long estimative = currentHLLState.cardinality();
		// int real = currentHLLState.getValues().size();
		// // error (= [estimated cardinality - true cardinality] / true cardinality)
		// double error = (double) (((double) (estimative - real)) / real);

		if (count != 0) {
			long estimative = this.cardinality.cardinality();
			double selectivity = Double.parseDouble(String.valueOf(estimative))
					/ Double.parseDouble(String.valueOf(count));
			double selectivityInc = Double.parseDouble(String.valueOf(estimative))
					/ Double.parseDouble(String.valueOf(count - (maxCount - increment)));
			System.out.println(
					"cardinality[" + estimative + "] count[" + count + "] selectivity[" + dec.format(selectivity)
							+ "] selectivity inc[" + dec.format(selectivityInc) + "] maxCount[" + maxCount + "]");
			// logger.info("count[" + count + "] cardinality[" + estimative + "]
			// selectivity[" + dec.format(selectivityInc) + "] + maxCount[" + maxCount +
			// "]");

			/**
			 * <pre>
			 * If selectivity is too short it is a good idea to increase the combiner.
			 * If selectivity is between 0.5 and 0.8 we don't change the combiner.
			 * If selectivity is higher than 0.8 we decrease the level of the combiner.
			 * If selectivity is equal 1 it means that all items are distinct on the 
			 * list and there is no reason to execute the combiner at all.
			 * </pre>
			 */
			if (selectivityInc <= 0.5) {
				if (maxCount + increment < LIMIT_MAX_COUNT) {
					maxCount = maxCount + increment;
				}
			} else if (selectivityInc > 0.5 && selectivityInc < 0.8) {
				// does nothing
			} else if (selectivityInc >= 0.8 && selectivityInc < 1) {
				if (maxCount - increment > LIMIT_MIN_COUNT) {
					maxCount = maxCount - increment;
				}
			} else if (selectivityInc == 1.0) {
				maxCount = 1;
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

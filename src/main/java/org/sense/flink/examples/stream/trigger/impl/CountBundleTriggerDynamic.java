package org.sense.flink.examples.stream.trigger.impl;

import java.util.Calendar;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Preconditions;
import org.sense.flink.examples.stream.trigger.BundleTrigger;
import org.sense.flink.examples.stream.trigger.BundleTriggerCallback;
import org.sense.flink.examples.stream.trigger.BundleTriggerDynamic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.clearspring.analytics.stream.frequency.IFrequency;

/**
 * A {@link BundleTrigger} that fires once the count of elements in a bundle
 * reaches the given count.
 */
public class CountBundleTriggerDynamic<K, T> implements BundleTriggerDynamic<K, T> {
	private static final Logger logger = LoggerFactory.getLogger(CountBundleTriggerDynamic.class);
	private static final long serialVersionUID = 8806579977649767427L;

	private final long LIMIT_MAX_COUNT = 1000;
	private final long LIMIT_MIN_COUNT = 1;
	private final long INCREMENT = 5;

	private long maxCount;
	private transient long count = 0;
	private transient BundleTriggerCallback callback;
	private transient IFrequency frequency;
	private transient long maxFrequencyCMS = 0;
	private transient long startTime;

	public CountBundleTriggerDynamic() throws Exception {
		initFrequencySketch();
		this.maxCount = LIMIT_MIN_COUNT;
		this.startTime = Calendar.getInstance().getTimeInMillis();
		Preconditions.checkArgument(this.maxCount > 0, "maxCount must be greater than 0");
	}

	@Override
	public void registerCallback(BundleTriggerCallback callback) {
		this.callback = Preconditions.checkNotNull(callback, "callback is null");
		this.startTime = Calendar.getInstance().getTimeInMillis();
		initFrequencySketch();
	}

	@Override
	public void onElement(K key, T element) throws Exception {
		// add key element on the HyperLogLog to infer the data-stream cardinality
		this.frequency.add((Long) key, 1);
		long itemCMS = this.frequency.estimateCount((Long) key);
		if (itemCMS > this.maxFrequencyCMS) {
			this.maxFrequencyCMS = itemCMS;
		}
		count++;
		long before = Calendar.getInstance().getTimeInMillis() - Time.minutes(2).toMilliseconds();
		if (count >= maxCount || before >= startTime) {
			if (before >= startTime) {
				System.out.println("Thread[" + Thread.currentThread().getId() + "] before >= startTime");
			}
			callback.finishBundle();
			reset();
		}
	}

	@Override
	public void reset() throws Exception {
		if (count != 0) {
			System.out.println("Thread[" + Thread.currentThread().getId() + "] frequencyCMS[" + maxFrequencyCMS
					+ "] maxCount[" + maxCount + "]");
			// if (this.maxFrequencyCMS > maxCount + (10% of the maxCount))
			if (this.maxFrequencyCMS > maxCount + (INCREMENT * 4)) {
				// necessary to apply combiner
				if (maxCount + INCREMENT < LIMIT_MAX_COUNT) {
					maxCount = maxCount + INCREMENT;
					resetFrequencySketch();
				}
			} else if (this.maxFrequencyCMS < (maxCount - (INCREMENT * 4))) {
				// this option will trigger only by timeout
				// reduce the combiner degree
				if (maxCount - INCREMENT > LIMIT_MIN_COUNT) {
					maxCount = maxCount - INCREMENT;
					resetFrequencySketch();
				}
			}
		}
		count = 0;
	}

	private void initFrequencySketch() {
		if (this.frequency == null) {
			this.frequency = new CountMinSketch(10, 5, 0);
		}
	}

	private void resetFrequencySketch() {
		this.frequency = new CountMinSketch(10, 5, 0);
		this.maxFrequencyCMS = 0;
		this.startTime = Calendar.getInstance().getTimeInMillis();
	}

	@Override
	public String explain() {
		return "CountBundleTrigger with size " + maxCount;
	}
}

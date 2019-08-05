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

	private final long LIMIT_MIN_COUNT = 1;
	private final long INCREMENT = 10;

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

	/**
	 * The Combiner is triggered when the count reaches the maxCount or by a timeout
	 */
	@Override
	public void onElement(K key, T element) throws Exception {
		// add key element on the HyperLogLog to infer the data-stream cardinality
		this.frequency.add((Long) key, 1);
		long itemCMS = this.frequency.estimateCount((Long) key);
		if (itemCMS > this.maxFrequencyCMS) {
			this.maxFrequencyCMS = itemCMS;
		}
		count++;
		long beforeTime = Calendar.getInstance().getTimeInMillis() - Time.seconds(30).toMilliseconds();
		if (count >= maxCount || beforeTime >= startTime) {
			callback.finishBundle();
		}
	}

	@Override
	public void reset() throws Exception {
		if (count != 0) {
			String msg = "Thread[" + Thread.currentThread().getId() + "] frequencyCMS[" + maxFrequencyCMS
					+ "] maxCount[" + maxCount + "]";
			if (maxFrequencyCMS > maxCount + INCREMENT) {
				// It is necessary to increase the combiner
				long diff = maxFrequencyCMS - maxCount;
				maxCount = maxCount + diff;
				msg = msg + " - INCREASING >>>";
				resetFrequencySketch();
			} else if (maxFrequencyCMS < maxCount - INCREMENT) {
				// It is necessary to reduce the combiner
				maxCount = maxFrequencyCMS + INCREMENT;
				msg = msg + " - DECREASING <<<";
				resetFrequencySketch();
			} else {
				msg = msg + " - HOLDING";
				this.startTime = Calendar.getInstance().getTimeInMillis();
			}
			// System.out.println(msg);
			logger.info(msg);
		}
		count = 0;
	}

	private void initFrequencySketch() {
		if (this.frequency == null) {
			this.frequency = new CountMinSketch(10, 5, 0);
		}
		this.maxFrequencyCMS = 0;
		this.startTime = Calendar.getInstance().getTimeInMillis();
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

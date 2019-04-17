package org.sense.flink.examples.stream.operators;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BundleTrigger} that fires once the count of elements in a bundle
 * reaches the given count.
 */
public class CountBundleTrigger<T> implements BundleTrigger<T> {

	private static final Logger logger = LoggerFactory.getLogger(CountBundleTrigger.class);
	private static final long serialVersionUID = -4343892739180507503L;

	private final long maxCount;
	private transient BundleTriggerCallback callback;
	private transient long count = 0;

	public CountBundleTrigger(long maxCount) {
		Preconditions.checkArgument(maxCount > 0, "maxCount must be greater than 0");
		this.maxCount = maxCount;
	}

	@Override
	public void registerCallback(BundleTriggerCallback callback) {
		this.callback = Preconditions.checkNotNull(callback, "callback is null");
	}

	@Override
	public void onElement(T element) throws Exception {
		count++;
		if (count >= maxCount) {
			callback.finishBundle();
			reset();
		}
	}

	@Override
	public void reset() {
		count = 0;
	}

	@Override
	public String explain() {
		return "CountBundleTrigger with size " + maxCount;
	}
}

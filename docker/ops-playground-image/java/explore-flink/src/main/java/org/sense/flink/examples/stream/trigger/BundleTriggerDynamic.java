package org.sense.flink.examples.stream.trigger;

import java.io.Serializable;

/**
 * A {@link BundleTriggerDynamic} determines when a bundle of input elements
 * should be evaluated and trigger the callback which registered previously.
 *
 * @param <T> The input element type.
 */
public interface BundleTriggerDynamic<K, T> extends Serializable {

	/**
	 * Register a callback which will be called once this trigger decides to finish
	 * this bundle.
	 */
	void registerCallback(BundleTriggerCallback callback);

	/**
	 * Called for every element that gets added to the bundle. If the trigger
	 * decides to start evaluate the input,
	 * {@link BundleTriggerCallback#finishBundle()} should be invoked.
	 *
	 * @param element The element that arrived.
	 */
	void onElement(final K key, final T element) throws Exception;

	/**
	 * Reset the trigger to its initiate status.
	 */
	void reset() throws Exception;

	String explain();
}

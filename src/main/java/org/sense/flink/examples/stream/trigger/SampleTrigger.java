package org.sense.flink.examples.stream.trigger;

import java.io.Serializable;

public interface SampleTrigger<T> extends Serializable {

	void registerCallback(SamplerTriggerCallback callback);

	void onElement(final T element) throws Exception;

	void reset();

	String explain();
}

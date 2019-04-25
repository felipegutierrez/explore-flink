package org.sense.flink.examples.stream.trigger;

public interface SamplerTriggerCallback {
	void emitSample() throws Exception;
}

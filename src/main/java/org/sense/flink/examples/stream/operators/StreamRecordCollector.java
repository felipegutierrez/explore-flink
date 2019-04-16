package org.sense.flink.examples.stream.operators;

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

/**
 * Wrapper around an {@link Output} for wrap {@code T} to {@link StreamRecord}.
 *
 * @param <T> The type of the elements that can be emitted.
 */
public class StreamRecordCollector<T> implements Collector<T> {

	private final StreamRecord<T> element = new StreamRecord<>(null);

	private final Output<StreamRecord<T>> underlyingOutput;

	public StreamRecordCollector(Output<StreamRecord<T>> output) {
		this.underlyingOutput = output;
	}

	@Override
	public void collect(T record) {
		underlyingOutput.collect(element.replace(record));
	}

	@Override
	public void close() {
		underlyingOutput.close();
	}
}

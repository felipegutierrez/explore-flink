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

	// private final Output<StreamRecord<T>> underlyingOutput;
	private final Output<StreamRecord<T>> output;

	// private final StreamRecord<T> element = new StreamRecord<>(null);
	private final StreamRecord<T> reuse;

	public StreamRecordCollector(Output<StreamRecord<T>> output) {
		// this.underlyingOutput = output;
		this.output = output;
		this.reuse = new StreamRecord<T>(null);
	}

	@Override
	public void collect(T record) {
		// underlyingOutput.collect(element.replace(record));
		output.collect(reuse.replace(record));
	}

	@Override
	public void close() {
		// underlyingOutput.close();
		output.close();
	}
}

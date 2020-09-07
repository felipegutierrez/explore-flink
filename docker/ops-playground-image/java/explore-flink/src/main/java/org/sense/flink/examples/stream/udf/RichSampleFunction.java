package org.sense.flink.examples.stream.udf;

import java.util.Map;

import javax.annotation.Nullable;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.util.Collector;

/**
 *
 * @param <K> The type of the key in the bundle map
 * @param <V> The type of the value in the bundle map
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the returned elements.
 */
public abstract class RichSampleFunction<K, V, IN, OUT> extends AbstractRichFunction implements Function {

	private static final long serialVersionUID = 6128385215081506903L;

	/**
	 * sample the tuples passing over this operator.
	 * 
	 * @param input the given input, not null
	 */
	public abstract V sampleInput(@Nullable V value, IN input);

	/**
	 * Called when the operator decides to emit the sample.
	 */
	public abstract void emitSample(Map<K, V> buffer, Collector<OUT> out) throws Exception;

}

package org.sense.flink.examples.stream.operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.util.Collector;

/**
 * Base interface for Map functions. Map functions take elements and transform
 * them, element wise. A Map function always produces a single result element
 * for each input element. Typical applications are parsing elements, converting
 * data types, or projecting out fields. Operations that produce multiple result
 * elements from a single input element can be implemented using the
 * {@link FlatMapFunction}.
 *
 * <p>
 * The basic syntax for using a MapFunction is as follows:
 * 
 * <pre>
 * {@code
 * DataSet<X> input = ...;
 *
 * DataSet<Y> result = input.map(new MyMapFunction());
 * }
 * </pre>
 *
 * @param <K> The type of the key in the bundle map
 * @param <V> The type of the value in the bundle map
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the returned elements.
 */
public abstract class MapBundleFunction<K, V, IN, OUT> implements Function {

	private static final long serialVersionUID = 3034447909940154208L;

	// protected List<V> outputs = new ArrayList<V>();
	// protected int finishCount = 0;

	/**
	 * Adds the given input to the given value, returning the new bundle value.
	 *
	 * @param value the existing bundle value, maybe null
	 * @param input the given input, not null
	 */
	public abstract V addInput(@Nullable V value, IN input);

	/**
	 * Called when a bundle is finished. Transform a bundle to zero, one, or more
	 * output elements.
	 */
	public abstract void finishBundle(Map<K, V> buffer, Collector<OUT> out) throws Exception;

}

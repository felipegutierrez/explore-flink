package org.sense.flink.examples.stream.operators;

import java.io.Serializable;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Function;

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
 * @param <T> Type of the input elements.
 * @param <O> Type of the returned elements.
 */
@Public
@FunctionalInterface
public interface MyMapFunction<T, O> extends Function, Serializable {

	/**
	 * The mapping method. Takes an element from the input data set and transforms
	 * it into exactly one element.
	 *
	 * @param value The input value.
	 * @return The transformed value
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception
	 *                   will cause the operation to fail and may trigger recovery.
	 */
	O map(T value) throws Exception;
}

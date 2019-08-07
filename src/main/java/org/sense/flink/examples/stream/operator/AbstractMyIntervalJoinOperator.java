package org.sense.flink.examples.stream.operator;

import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;

public abstract class AbstractMyIntervalJoinOperator<K, T1, T2, OUT>
		extends AbstractUdfStreamOperator<OUT, ProcessJoinFunction<T1, T2, OUT>>
		implements TwoInputStreamOperator<T1, T2, OUT>, Triggerable<K, String> {
	private static final long serialVersionUID = 621798955806366389L;

	public AbstractMyIntervalJoinOperator(ProcessJoinFunction<T1, T2, OUT> userFunction) {
		super(userFunction);
	}
}

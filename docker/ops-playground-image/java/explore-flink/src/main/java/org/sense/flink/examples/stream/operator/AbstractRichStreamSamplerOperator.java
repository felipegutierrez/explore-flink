package org.sense.flink.examples.stream.operator;

import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.sense.flink.examples.stream.trigger.SamplerTriggerCallback;
import org.sense.flink.examples.stream.udf.RichSampleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRichStreamSamplerOperator<K, V, IN, OUT>
		extends AbstractUdfStreamOperator<OUT, RichSampleFunction<K, V, IN, OUT>>
		implements OneInputStreamOperator<IN, OUT>, SamplerTriggerCallback {

	private static final long serialVersionUID = 1294502664005367886L;
	private static final Logger logger = LoggerFactory.getLogger(AbstractRichStreamSamplerOperator.class);

	// buffer

	// sample component which updates the buffer

	// emission component which produces the output or the operator

	/** Output for stream records. */
	private transient TimestampedCollector<OUT> collector;

	public AbstractRichStreamSamplerOperator(RichSampleFunction<K, V, IN, OUT> function) {
		super(function);
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
	}

	/**
	 * Get the key for current processing element, which will be used as the map
	 * bundle's key.
	 */
	protected abstract K getKey(final IN input) throws Exception;

	@Override
	public void emitSample() throws Exception {
	}
}

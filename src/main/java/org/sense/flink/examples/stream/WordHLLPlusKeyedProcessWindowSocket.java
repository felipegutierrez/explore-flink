package org.sense.flink.examples.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.util.HyperLogLogPlusState;

/**
 * On the terminal execute "nc -lk 9000", run this class and type words back on
 * the terminal
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class WordHLLPlusKeyedProcessWindowSocket {

	public static void main(String[] args) throws Exception {
		new WordHLLPlusKeyedProcessWindowSocket();
	}

	public WordHLLPlusKeyedProcessWindowSocket() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// @formatter:off
		env.socketTextStream("localhost", 9000)
				.flatMap(new SplitterFlatMap())
				.keyBy(new NullByteKeySelector<String>())
				.process(new TimeOutDistinctAndCountFunction(5 * 1000))
				.print();
		// @formatter:on

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("WordHLLPlusKeyedProcessWindowSocket");
	}

	public static class SplitterFlatMap implements FlatMapFunction<String, String> {
		private static final long serialVersionUID = 3121588720675797629L;

		@Override
		public void flatMap(String sentence, Collector<String> out) throws Exception {
			for (String word : sentence.split(" ")) {
				out.collect(word);
			}
		}
	}

	public static class TimeOutDistinctAndCountFunction extends KeyedProcessFunction<Byte, String, String> {
		private static final long serialVersionUID = 7289761960983988878L;
		// delay after which an alert flag is thrown
		private final long timeOut;
		// state to remember the last timer set
		private transient ValueState<HyperLogLogPlusState> hllState;

		public TimeOutDistinctAndCountFunction(long timeOut) {
			this.timeOut = timeOut;
		}

		@Override
		public void open(Configuration conf) {
			// setup timer and HLL state
			ValueStateDescriptor<HyperLogLogPlusState> descriptorHLL = new ValueStateDescriptor<>("hllState",
					HyperLogLogPlusState.class);
			hllState = getRuntimeContext().getState(descriptorHLL);
		}

		@Override
		public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
			// get current time and compute timeout time
			long currentTime = ctx.timerService().currentProcessingTime();
			long timeoutTime = currentTime + timeOut;
			// register timer for timeout time
			ctx.timerService().registerProcessingTimeTimer(timeoutTime);

			// update HLL state
			HyperLogLogPlusState currentHLLState = hllState.value();
			if (currentHLLState == null) {
				currentHLLState = new HyperLogLogPlusState();
			}
			currentHLLState.offer(value);

			// remember timeout time
			currentHLLState.setLastTimer(timeoutTime);
			hllState.update(currentHLLState);
			System.out.println("----------- process");
			System.out.println("currentTime: " + currentTime);
			System.out.println("timeoutTime: " + timeoutTime);
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
			// System.out.println("onTimer: " + timestamp);
			// check if this was the last timer we registered
			HyperLogLogPlusState currentHLLState = hllState.value();
			System.out.println("--------------------------------- onTimer");
			System.out.println("timestamp: " + timestamp);
			System.out.println("LastTimer: " + currentHLLState.getLastTimer());
			if (timestamp == currentHLLState.getLastTimer()) {
				// it was, so no data was received afterwards. fire an alert.
				out.collect("estimate cardinality: " + currentHLLState.cardinality());
			}
		}
	}
}

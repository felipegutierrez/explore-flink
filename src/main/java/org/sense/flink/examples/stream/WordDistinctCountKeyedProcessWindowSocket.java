package org.sense.flink.examples.stream;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * On the terminal execute "nc -lk 9000", run this class and type words back on
 * the terminal
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class WordDistinctCountKeyedProcessWindowSocket {

	public static void main(String[] args) throws Exception {
		new WordDistinctCountKeyedProcessWindowSocket();
	}

	public WordDistinctCountKeyedProcessWindowSocket() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// @formatter:off
		env.socketTextStream("localhost", 9000)
				.flatMap(new SplitterFlatMap())
				.keyBy(new WordKeySelector())
				.process(new DistinctCountProcessFunction())
				.print();
		// @formatter:on

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("WordDistinctCountKeyedProcessWindowSocket");
	}

	public static class SplitterFlatMap implements FlatMapFunction<String, Tuple2<Integer, String>> {
		private static final long serialVersionUID = 3121588720675797629L;

		@Override
		public void flatMap(String sentence, Collector<Tuple2<Integer, String>> out) throws Exception {
			for (String word : sentence.split(" ")) {
				out.collect(new Tuple2<Integer, String>(0, word));
			}
		}
	}

	public static class WordKeySelector implements KeySelector<Tuple2<Integer, String>, Integer> {
		private static final long serialVersionUID = 2787589690596587044L;

		@Override
		public Integer getKey(Tuple2<Integer, String> value) throws Exception {
			return value.f0;
		}
	}

	public static class DistinctCountWithTimestamp {
		public List<String> distinctWords;
		public Integer distinctCount;
		public long lastModified;

		public DistinctCountWithTimestamp() {
			this.distinctWords = new ArrayList<String>();
			this.distinctCount = 0;
			this.lastModified = 0L;
		}
	}

	public static class DistinctCountProcessFunction
			extends KeyedProcessFunction<Integer, Tuple2<Integer, String>, Tuple2<String, Integer>> {
		private static final long serialVersionUID = -2401464040227250930L;
		private ValueState<DistinctCountWithTimestamp> state;

		@Override
		public void open(Configuration parameters) throws Exception {
			state = getRuntimeContext()
					.getState(new ValueStateDescriptor<>("myState", DistinctCountWithTimestamp.class));
		}

		@Override
		public void processElement(Tuple2<Integer, String> value,
				KeyedProcessFunction<Integer, Tuple2<Integer, String>, Tuple2<String, Integer>>.Context ctx,
				Collector<Tuple2<String, Integer>> out) throws Exception {
			Integer key = value.f0;
			String word = value.f1;
			// retrieve the current state
			DistinctCountWithTimestamp current = state.value();
			if (current == null) {
				current = new DistinctCountWithTimestamp();
				// update the state's count
				current.distinctWords.add(word);
				current.distinctCount++;
			} else {
				if (!current.distinctWords.contains(word)) {
					// update the state's count
					current.distinctWords.add(word);
					current.distinctCount++;
				}
			}
			// set the state's timestamp to the record's assigned event time timestamp
			current.lastModified = ctx.timestamp();

			// write the state back
			state.update(current);

			// schedule the next timer 5 seconds from the current event time
			ctx.timerService().registerEventTimeTimer(current.lastModified + 5000);
		}

		@Override
		public void onTimer(long timestamp,
				KeyedProcessFunction<Integer, Tuple2<Integer, String>, Tuple2<String, Integer>>.OnTimerContext ctx,
				Collector<Tuple2<String, Integer>> out) throws Exception {
			// System.out.println("onTimer");
			// get the state for the key that scheduled the timer
			DistinctCountWithTimestamp result = state.value();

			// check if this is an outdated timer or the latest timer
			if (timestamp == result.lastModified + 5000) {
				String distincWords = "";
				for (String word : result.distinctWords) {
					distincWords = distincWords + word + "-";
				}
				// emit the state on timeout
				out.collect(
						new Tuple2<String, Integer>("distrinct values [" + distincWords + "]", result.distinctCount));
			}
		}
	}
}

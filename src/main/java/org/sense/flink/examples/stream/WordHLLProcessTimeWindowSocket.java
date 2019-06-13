package org.sense.flink.examples.stream;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

/**
 * On the terminal execute "nc -lk 9000", run this class and type words back on
 * the terminal
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class WordHLLProcessTimeWindowSocket {

	public static void main(String[] args) throws Exception {
		new WordHLLProcessTimeWindowSocket();
	}

	public WordHLLProcessTimeWindowSocket() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		Time time = Time.seconds(5);

		// @formatter:off
		env.socketTextStream("localhost", 9000)
				.flatMap(new SplitterFlatMap())
				.keyBy(new WordKeySelector())
				.timeWindow(time)
				.reduce(new HLLRichReduceFunction())
				// .process(new DistinctProcessWindowFunction())
				// .timeWindowAll(time)
				// .reduce(new CountReduceFunction())
				.print();
		// @formatter:on

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("WordHLLProcessTimeWindowSocket");
	}

	public static class HLLRichReduceFunction extends RichReduceFunction<Tuple2<Integer, String>> {
		@Override
		public Tuple2<Integer, String> reduce(Tuple2<Integer, String> value1, Tuple2<Integer, String> value2)
				throws Exception {
			return null;
		}
	}

	public static class SplitterFlatMap implements FlatMapFunction<String, Tuple2<Integer, String>> {
		private static final long serialVersionUID = 3121588720675797629L;

		@Override
		public void flatMap(String sentence, Collector<Tuple2<Integer, String>> out) throws Exception {
			for (String word : sentence.split(" ")) {
				out.collect(new Tuple2<Integer, String>(1, word));
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

	public static class DistinctProcessWindowFunction
			extends ProcessWindowFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Integer, TimeWindow> {
		private static final long serialVersionUID = -712802393634597999L;

		@Override
		public void process(Integer key,
				ProcessWindowFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Integer, TimeWindow>.Context ctx,
				Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
			Tuple2<Integer, String> value = values.iterator().next();
			out.collect(Tuple2.of(1, value.f1));
		}
	}

	public static class CountReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 8047191633772408164L;

		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2)
				throws Exception {
			return Tuple2.of(value1.f0 + "-" + value2.f0, value1.f1 + value2.f1);
		}
	}

	public static class HLLWithTimestamp implements Serializable {
		private static final long serialVersionUID = 175895958203648517L;
		public HyperLogLog hyperLogLog;
		public List<String> distinctWords;
		public Integer distinctCount;
		public long lastModified;

		public HLLWithTimestamp() {
			this.hyperLogLog = new HyperLogLog(16);
			this.distinctWords = new ArrayList<String>();
			this.distinctCount = 0;
			this.lastModified = 0L;
		}

		public boolean offer(Object value) {
			return hyperLogLog.offer(value);
		}

		public long cardinality() {
			return hyperLogLog.cardinality();
		}
	}

	public static class HLLProcessFunction
			extends KeyedProcessFunction<Integer, Tuple2<Integer, String>, Tuple3<String, Integer, Long>> {
		private static final long serialVersionUID = -2401464040227250930L;
		private ValueState<HLLWithTimestamp> state;

		@Override
		public void open(Configuration parameters) throws Exception {
			state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", HLLWithTimestamp.class));
		}

		@Override
		public void processElement(Tuple2<Integer, String> value,
				KeyedProcessFunction<Integer, Tuple2<Integer, String>, Tuple3<String, Integer, Long>>.Context ctx,
				Collector<Tuple3<String, Integer, Long>> out) throws Exception {
			Integer key = value.f0;
			String word = value.f1;
			// retrieve the current state
			HLLWithTimestamp current = state.value();
			if (current == null) {
				current = new HLLWithTimestamp();
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
			// add a value on the HLL sketch
			current.offer(word);
			// set the state's timestamp to the record's assigned event time timestamp
			current.lastModified = ctx.timestamp();

			// write the state back
			state.update(current);

			// schedule the next timer 5 seconds from the current event time
			ctx.timerService().registerEventTimeTimer(current.lastModified + 5000);
		}

		@Override
		public void onTimer(long timestamp,
				KeyedProcessFunction<Integer, Tuple2<Integer, String>, Tuple3<String, Integer, Long>>.OnTimerContext ctx,
				Collector<Tuple3<String, Integer, Long>> out) throws Exception {
			// get the state for the key that scheduled the timer
			HLLWithTimestamp result = state.value();

			// check if this is an outdated timer or the latest timer
			if (timestamp == result.lastModified + 5000) {
				String distincWords = "";
				for (String word : result.distinctWords) {
					distincWords = distincWords + word + "-";
				}
				double err = Math.abs(result.cardinality() - result.distinctCount) / (double) result.distinctCount;
				System.out.println("-------------------------");
				System.out.println("exact cardinality    : " + result.distinctCount);
				System.out.println("estimated cardinality: " + result.cardinality());
				System.out.println("Error                : " + err);
				// emit the state on timeout
				out.collect(new Tuple3<String, Integer, Long>("distrinct values [" + distincWords + "]",
						result.distinctCount, result.cardinality()));
			}
		}
	}
}

package org.sense.flink.examples.stream.socket;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * On the terminal execute "nc -lk 9000", run this class and type words back on
 * the terminal
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class WordDistinctCountAggregateWindowSocket {

	public static void main(String[] args) throws Exception {
		new WordDistinctCountAggregateWindowSocket();
	}

	public WordDistinctCountAggregateWindowSocket() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// @formatter:off
		env.socketTextStream("localhost", 9000)
				.flatMap(new SplitterFlatMap())
				.map(new SwapMap())
				.keyBy(new WordKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.aggregate(new DistinctCountAggregateFunction())
				.print();
		// @formatter:on

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("WordDistinctCountAggregateWindowSocket");
	}

	public static class SplitterFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 3121588720675797629L;

		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
			for (String word : sentence.split(" ")) {
				out.collect(new Tuple2<String, Integer>(word, 1));
			}
		}
	}

	public static class SwapMap implements MapFunction<Tuple2<String, Integer>, Tuple2<Integer, String>> {
		private static final long serialVersionUID = -1392476272305784921L;

		@Override
		public Tuple2<Integer, String> map(Tuple2<String, Integer> value) throws Exception {
			return Tuple2.of(value.f1, value.f0);
		}
	}

	public static class WordKeySelector implements KeySelector<Tuple2<Integer, String>, Integer> {
		private static final long serialVersionUID = 2787589690596587044L;

		@Override
		public Integer getKey(Tuple2<Integer, String> value) throws Exception {
			return value.f0;
		}
	}

	public static class DistinctCountAggregateFunction
			implements AggregateFunction<Tuple2<Integer, String>, DistinctCountWithTimestamp, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 996334987119123032L;

		@Override
		public DistinctCountWithTimestamp createAccumulator() {
			System.out.println("createAccumulator");
			return new DistinctCountWithTimestamp(new HashSet<String>(), 0, System.currentTimeMillis());
		}

		@Override
		public DistinctCountWithTimestamp add(Tuple2<Integer, String> value, DistinctCountWithTimestamp accumulator) {
			System.out.println("add");
			accumulator.distinctWords.add(value.f1);
			accumulator.distinctCount = accumulator.distinctWords.size();
			return accumulator;
		}

		@Override
		public Tuple2<String, Integer> getResult(DistinctCountWithTimestamp accumulator) {
			System.out.println("getResult");
			String items = "";
			for (String item : accumulator.distinctWords) {
				items = items + item + "-";
			}
			return Tuple2.of(items, accumulator.distinctCount);
		}

		@Override
		public DistinctCountWithTimestamp merge(DistinctCountWithTimestamp a, DistinctCountWithTimestamp b) {
			System.out.println("merge");
			return null;
		}
	}

	public static class DistinctCountWithTimestamp {
		public Set<String> distinctWords;
		public Integer distinctCount;
		public long lastModified;

		public DistinctCountWithTimestamp() {
			this.distinctWords = new HashSet<String>();
			this.distinctCount = 0;
			this.lastModified = 0L;
		}

		public DistinctCountWithTimestamp(Set<String> distinctWords, Integer distinctCount, long lastModified) {
			this.distinctWords = distinctWords;
			this.distinctCount = distinctCount;
			this.lastModified = lastModified;
		}

		@Override
		public String toString() {
			return "DistinctCountWithTimestamp [distinctWords=" + distinctWords + ", distinctCount=" + distinctCount
					+ ", lastModified=" + lastModified + "]";
		}
	}
}

package org.sense.flink.examples.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
public class WordCountDistinctSocketFilterQEP {

	public static void main(String[] args) throws Exception {
		new WordCountDistinctSocketFilterQEP();
	}

	public WordCountDistinctSocketFilterQEP() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// @formatter:off
		env.socketTextStream("localhost", 9000)
				.flatMap(new SplitterFlatMap())
				.keyBy(new WordKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.reduce(new CountReduceFunction())
				.map(new WordSwapMapFunction())
				.keyBy(0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.reduce(new CountDistinctFunction())
				.print();
		// @formatter:on

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("WordCountDistinctSocketFilterQEP");
	}

	public static class WordSwapMapFunction implements MapFunction<Tuple2<String, Integer>, Tuple2<Integer, String>> {
		private static final long serialVersionUID = 5148172163266330182L;

		@Override
		public Tuple2<Integer, String> map(Tuple2<String, Integer> value) throws Exception {
			return Tuple2.of(1, value.f0);
		}
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

	public static class WordKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
		private static final long serialVersionUID = 2787589690596587044L;

		@Override
		public String getKey(Tuple2<String, Integer> value) throws Exception {
			return value.f0;
		}
	}

	public static class CountReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 8541031982462158730L;

		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2)
				throws Exception {
			return Tuple2.of(value1.f0, value1.f1 + value2.f1);
		}
	}

	public static class CountDistinctFunction implements ReduceFunction<Tuple2<Integer, String>> {
		private static final long serialVersionUID = -7077952757215699563L;

		@Override
		public Tuple2<Integer, String> reduce(Tuple2<Integer, String> value1, Tuple2<Integer, String> value2)
				throws Exception {
			return Tuple2.of(value1.f0 + value2.f0, value1.f1 + "-" + value2.f1);
		}
	}
}

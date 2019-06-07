package org.sense.flink.examples.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
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
		// DataStream<Tuple2<String, Integer>> dataStream =
		env.socketTextStream("localhost", 9000)
				.flatMap(new SplitterFlatMap())
				.keyBy(new MyKeySelector())
				// .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
				.timeWindow(Time.seconds(5))
				// .reduce(new CountReduceFunction())
				.reduce(new CountReduceFunction(), new CountDistinctProcessFunction())
				.print();
		// @formatter:on

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");
		// dataStream.print();

		env.execute("WordCountDistinctSocketFilterQEP");
	}

	public static class SplitterFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {

		private static final long serialVersionUID = -6155646115486510443L;

		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
			for (String word : sentence.split(" ")) {
				out.collect(new Tuple2<String, Integer>(word, 1));
			}
		}
	}

	public static class MyKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
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

	public static class CountDistinctProcessFunction
			extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

		private static final long serialVersionUID = 8056206953318511310L;
		private ValueState<Tuple2<String, Integer>> modelState;

		public void open(Configuration parameters) throws Exception {
			ValueStateDescriptor<Tuple2<String, Integer>> descriptor = new ValueStateDescriptor<>("modelState",
					TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class));
			this.modelState = getRuntimeContext().getState(descriptor);
		}

		@Override
		public void process(String key,
				ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>.Context ctx,
				Iterable<Tuple2<String, Integer>> value, Collector<Tuple2<String, Integer>> out) throws Exception {
			String keys = null;
			Integer qtd = 0;
			if (modelState.value() != null) {
				Tuple2<String, Integer> state = modelState.value();
				keys = state.f0;
				qtd = state.f1;
			} else {

			}

			for (Tuple2<String, Integer> tuple2 : value) {
				keys = keys + "," + tuple2.f0;
				qtd++;
			}
			modelState.update(Tuple2.of(keys, qtd));
			out.collect(Tuple2.of(key, qtd));
		}
	}
}

package org.sense.flink.examples.stream.edgent;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TemperatureAverageExample {

	private boolean checkpointEnable = false;
	private long checkpointInterval = 10000;
	private CheckpointingMode checkpointMode = CheckpointingMode.EXACTLY_ONCE;

	public TemperatureAverageExample() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.getConfig().disableSysoutLogging();

		// Tuple of (TYPE, ID, READING)
		DataStream<Tuple3<Integer, Integer, Integer>> input = env.fromElements(new Tuple3<>(0, 0, 10),
				new Tuple3<>(0, 0, 11), new Tuple3<>(0, 0, 12), new Tuple3<>(1, 0, 113), new Tuple3<>(0, 0, 114),
				new Tuple3<>(0, 0, 16), new Tuple3<>(2, 0, 19), new Tuple3<>(0, 0, 18), new Tuple3<>(1, 0, 15),
				new Tuple3<>(0, 0, 17), new Tuple3<>(2, 0, 20), new Tuple3<>(0, 0, 21));

		input.keyBy(1).flatMap(
				new RichFlatMapFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
					private final ValueStateDescriptor<Integer> count = new ValueStateDescriptor<>("count",
							IntSerializer.INSTANCE, 0);

					private final ReducingStateDescriptor<Integer> max = new ReducingStateDescriptor<>("max",
							new ReduceFunction<Integer>() {
								@Override
								public Integer reduce(Integer value1, Integer value2) throws Exception {
									return Math.max(value1, value2);
								}
							}, IntSerializer.INSTANCE);

					@Override
					public void flatMap(Tuple3<Integer, Integer, Integer> value,
							Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {

						if (value.f0 == 0) {
							ValueState<Integer> countState = getRuntimeContext().getState(count);

							countState.update(countState.value() + 1);

							getRuntimeContext().getReducingState(max).add(value.f2);
						} else if (value.f0 == 1) {
							ReducingState<Integer> maxState = getRuntimeContext().getReducingState(max);

							out.collect(new Tuple3<>(value.f0, value.f1, maxState.get()));
						} else if (value.f0 == 2) {
							ValueState<Integer> countState = getRuntimeContext().getState(count);

							out.collect(new Tuple3<>(value.f0, value.f1, countState.value()));
						}
					}
				}).print();

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("TemperatureAverageExample");
	}

	public static void main(String[] args) throws Exception {
		new TemperatureAverageExample();
	}
}

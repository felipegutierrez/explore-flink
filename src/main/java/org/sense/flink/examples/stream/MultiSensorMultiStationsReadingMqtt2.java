package org.sense.flink.examples.stream;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.mqtt.MqttSensorConsumer;
import org.sense.flink.util.CountMinSketch;

public class MultiSensorMultiStationsReadingMqtt2 {

	public MultiSensorMultiStationsReadingMqtt2() throws Exception {

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// obtain execution environment, run this example in "ingestion time"
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<MqttSensor> streamStation01 = env.addSource(new MqttSensorConsumer("topic-station-01"));
		DataStream<MqttSensor> streamStation02 = env.addSource(new MqttSensorConsumer("topic-station-02"));

		DataStream<MqttSensor> streamStations = streamStation01.union(streamStation02);
		// streamStations.print();

		// @formatter:off
		streamStations.filter(new SensorFilter(new String[]{"COUNT_PE"}))
				.map(new TrainStationMapper())
				.keyBy(0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.aggregate(new AverageAggregator())
				.print();
		
		streamStations.filter(new SensorFilter(new String[]{"COUNT_TI"}))
				.map(new TrainStationMapper())
				.keyBy(0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.aggregate(new AverageAggregator())
				.print();
		
		streamStations.filter(new SensorFilter(new String[]{"COUNT_TR"}))
				.map(new TrainStationMapper())
				.keyBy(0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.aggregate(new AverageAggregator())
				.print();

		streamStations.filter(new SensorFilter(new String[]{"COUNT_PE", "COUNT_TI", "COUNT_TR"}))
				.map(new TrainStationMapper())
				.keyBy(new MyKeySelector())	
				.timeWindow(Time.seconds(5))
				.aggregate(new AverageAggregator(), new MyProcessWindowFunction())
				.print();
		// @formatter:on

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("MultiSensorMultiStationsReadingMqtt2");
	}

	public static class MyKeySelector implements
			KeySelector<Tuple3<Integer, Tuple5<Integer, String, Integer, String, Integer>, Double>, Integer> {

		private static final long serialVersionUID = 2475747880505867591L;

		@Override
		public Integer getKey(Tuple3<Integer, Tuple5<Integer, String, Integer, String, Integer>, Double> value)
				throws Exception {
			return value.f0;
		}
	}

	public static class SensorFilter implements FilterFunction<MqttSensor> {

		private static final long serialVersionUID = 7991908941095866364L;
		private String[] filter;

		public SensorFilter(String[] filter) {
			this.filter = filter;
		}

		@Override
		public boolean filter(MqttSensor value) throws Exception {
			if (filter != null && filter.length > 0) {
				for (int i = 0; i < filter.length; i++) {
					if (filter[i].equals(value.getKey().f1)) {
						return true;
					}
				}
			}
			return false;
		}
	}

	public static class TrainStationMapper implements
			MapFunction<MqttSensor, Tuple3<Integer, Tuple5<Integer, String, Integer, String, Integer>, Double>> {

		private static final long serialVersionUID = -5565228597255633611L;

		@Override
		public Tuple3<Integer, Tuple5<Integer, String, Integer, String, Integer>, Double> map(MqttSensor value)
				throws Exception {
			Integer sensorId = value.getKey().f0;
			String sensorType = value.getKey().f1;
			Integer platformId = value.getKey().f2;
			String platformType = value.getKey().f3;
			Integer stationKey = value.getKey().f4;
			Double v = value.getValue();
			return Tuple3.of(stationKey, Tuple5.of(sensorId, sensorType, platformId, platformType, stationKey), v);
		}
	}

	public static class AverageAggregator implements
			AggregateFunction<Tuple3<Integer, Tuple5<Integer, String, Integer, String, Integer>, Double>, Tuple4<Double, Long, Integer, String>, Tuple2<String, Double>> {

		private static final long serialVersionUID = 7233937097358437044L;

		@Override
		public Tuple4<Double, Long, Integer, String> createAccumulator() {
			return new Tuple4<>(0.0, 0L, 0, "");
		}

		@Override
		public Tuple4<Double, Long, Integer, String> add(
				Tuple3<Integer, Tuple5<Integer, String, Integer, String, Integer>, Double> value,
				Tuple4<Double, Long, Integer, String> accumulator) {
			return new Tuple4<>(accumulator.f0 + value.f2, accumulator.f1 + 1L, value.f1.f4, value.f1.f1);
		}

		@Override
		public Tuple2<String, Double> getResult(Tuple4<Double, Long, Integer, String> accumulator) {
			return new Tuple2<>(accumulator.f3 + " " + accumulator.f2 + " ",
					((double) accumulator.f0) / accumulator.f1);
		}

		@Override
		public Tuple4<Double, Long, Integer, String> merge(Tuple4<Double, Long, Integer, String> a,
				Tuple4<Double, Long, Integer, String> b) {
			return new Tuple4<>(a.f0 + b.f0, a.f1 + b.f1, a.f2, a.f3);
		}
	}

	/**
	 * This class holds the state of all windows since the beginning of the data
	 * stream processing.
	 * 
	 * <pre>
	 * The output of the UDF is <ID, GLOBAL, AVG> where:
	 * 
	 * ID: sensor Id + station Id
	 * GLOBAL: amount of average took over global computation
	 * AVG: average of the sensor Id from the previous AggregationFunction
	 * </pre>
	 * 
	 * @author Felipe Oliveira Gutierrez
	 */
	public static class MyProcessWindowFunction extends
			ProcessWindowFunction<Tuple2<String, Double>, Tuple3<String, Integer, Double>, Integer, TimeWindow> {

		private static final long serialVersionUID = -40874489412082797L;

		private ValueStateDescriptor<CountMinSketch> descriptorGlobal = new ValueStateDescriptor<>(
				"countMinSketchState", CountMinSketch.class);

		private ValueStateDescriptor<Tuple2<String, Double>> descriptorWindow = new ValueStateDescriptor<>("state",
				TupleTypeInfo.getBasicTupleTypeInfo(String.class, Double.class));

		@Override
		public void process(Integer key,
				ProcessWindowFunction<Tuple2<String, Double>, Tuple3<String, Integer, Double>, Integer, TimeWindow>.Context context,
				Iterable<Tuple2<String, Double>> counts, Collector<Tuple3<String, Integer, Double>> out)
				throws Exception {

			// This is my state over all windows since the beginning of the data stream
			// processing
			ValueState<CountMinSketch> stateGlobal = context.globalState().getState(descriptorGlobal);

			CountMinSketch currentGlobalValue = stateGlobal.value();
			if (currentGlobalValue == null) {
				currentGlobalValue = new CountMinSketch();
			}
			Tuple2<String, Double> v = Iterables.getOnlyElement(counts);
			currentGlobalValue.updateSketchAsync(v.f0);

			// update the global state with the value of the current window
			stateGlobal.update(currentGlobalValue);

			// update the result of the ProcessWindowFunction
			out.collect(Tuple3.of(v.f0, currentGlobalValue.getFrequencyFromSketch(v.f0), v.f1));
		}
	}
}

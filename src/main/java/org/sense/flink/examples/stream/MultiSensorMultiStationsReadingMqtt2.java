package org.sense.flink.examples.stream;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.mqtt.MqttSensorConsumer;
import org.sense.flink.util.CountMinSketch;

public class MultiSensorMultiStationsReadingMqtt2 {

	public MultiSensorMultiStationsReadingMqtt2() throws Exception {

		final CountMinSketch countMinSketch = new CountMinSketch();

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// obtain execution environment, run this example in "ingestion time"
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<MqttSensor> streamStation01 = env.addSource(new MqttSensorConsumer("topic-station-01"));
		DataStream<MqttSensor> streamStation02 = env.addSource(new MqttSensorConsumer("topic-station-02"));

		DataStream<MqttSensor> streamStations = streamStation01.union(streamStation02);
		// streamStations.print();

		// @formatter:off
		streamStations.filter(new SensorFilter("COUNT_PE"))
				.map(new TrainStationMapper())
				.keyBy(0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.aggregate(new AverageAggregator("COUNT_PE", countMinSketch))
				.print();
		
		streamStations.filter(new SensorFilter("COUNT_TI"))
				.map(new TrainStationMapper())
				.keyBy(0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.aggregate(new AverageAggregator("COUNT_TI", countMinSketch))
				.print();
		
		streamStations.filter(new SensorFilter("COUNT_TR"))
				.map(new TrainStationMapper())
				.keyBy(0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.aggregate(new AverageAggregator("COUNT_TR", countMinSketch))
				.print();
		// @formatter:on

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("MultiSensorMultiStationsReadingMqtt2");
	}

	public static class SensorFilter implements FilterFunction<MqttSensor> {

		private static final long serialVersionUID = 7991908941095866364L;
		private String filter;

		public SensorFilter(String filter) {
			this.filter = filter;
		}

		@Override
		public boolean filter(MqttSensor value) throws Exception {
			if (value.getKey().f1.equals(filter)) {
				return true;
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
			AggregateFunction<Tuple3<Integer, Tuple5<Integer, String, Integer, String, Integer>, Double>, Tuple3<Double, Long, Integer>, Tuple2<String, Double>> {

		private static final long serialVersionUID = 7233937097358437044L;
		private String functionName;
		private CountMinSketch countMinSketch;

		public AverageAggregator(String functionName, CountMinSketch countMinSketch) {
			this.functionName = functionName;
			this.countMinSketch = countMinSketch;
		}

		@Override
		public Tuple3<Double, Long, Integer> createAccumulator() {
			return new Tuple3<>(0.0, 0L, 0);
		}

		@Override
		public Tuple3<Double, Long, Integer> add(
				Tuple3<Integer, Tuple5<Integer, String, Integer, String, Integer>, Double> value,
				Tuple3<Double, Long, Integer> accumulator) {

			if (value.f1.f1.equals("COUNT_PE")) {
				// int count = (int) Math.round(value.f2);
				countMinSketch.updateSketch("COUNT_PE");
			} else if (value.f1.f1.equals("COUNT_TI")) {
				// int count = (int) Math.round(value.f2);
				countMinSketch.updateSketch("COUNT_TI");
			} else if (value.f1.f1.equals("COUNT_TR")) {
				// int count = (int) Math.round(value.f2);
				countMinSketch.updateSketch("COUNT_TR");
			}

			return new Tuple3<>(accumulator.f0 + value.f2, accumulator.f1 + 1L, value.f1.f4);
		}

		@Override
		public Tuple2<String, Double> getResult(Tuple3<Double, Long, Integer> accumulator) {

			String label = "";
			int frequency = 0;
			if (functionName.equals("COUNT_PE")) {
				label = "PEOPLE average on train station";
				frequency = countMinSketch.getFrequencyFromSketch("COUNT_PE");

			} else if (functionName.equals("COUNT_TI")) {
				label = "TICKETS average on train station";
				frequency = countMinSketch.getFrequencyFromSketch("COUNT_TI");

			} else if (functionName.equals("COUNT_TR")) {
				label = "TRAIN average on train station";
				frequency = countMinSketch.getFrequencyFromSketch("COUNT_TR");
			}

			return new Tuple2<>(label + "[" + accumulator.f2 + "] reads[" + frequency + "]",
					((double) accumulator.f0) / accumulator.f1);
		}

		@Override
		public Tuple3<Double, Long, Integer> merge(Tuple3<Double, Long, Integer> a, Tuple3<Double, Long, Integer> b) {
			return new Tuple3<>(a.f0 + b.f0, a.f1 + b.f1, a.f2);
		}
	}
}

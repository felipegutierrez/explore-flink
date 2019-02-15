package org.sense.flink.examples.stream;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.mqtt.MqttSensorConsumer;

public class MultiSensorMultiStationsReadingMqtt2 {

	public MultiSensorMultiStationsReadingMqtt2() throws Exception {

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// obtain execution environment, run this example in "ingestion time"
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<MqttSensor> streamStation01 = env.addSource(new MqttSensorConsumer("topic-station-01"));
		DataStream<MqttSensor> streamStation02 = env.addSource(new MqttSensorConsumer("topic-station-02"));

		DataStream<MqttSensor> streamStations = streamStation01.union(streamStation02);
		streamStations.print();

		// @formatter:off
		streamStations.filter(new StationPeopleCountFilter())
				.map(new TrainStationMapper())
				.keyBy(0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.aggregate(new AverageAggregator())
				.print();
		
		streamStations.filter(new StationTicketCountFilter())
				.map(new TrainStationMapper())
				.keyBy(0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.aggregate(new AverageAggregator())
				.print();
		// @formatter:on

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("MultiSensorMultiStationsReadingMqtt2");
	}

	public static class StationPeopleCountFilter implements FilterFunction<MqttSensor> {

		private static final long serialVersionUID = 7991908941095866364L;

		@Override
		public boolean filter(MqttSensor value) throws Exception {
			if (value.getKey().f1.equals("COUNTER_PEOPLE") && value.getKey().f2.equals(0)) {
				return true;
			}
			return false;
		}
	}

	public static class StationTicketCountFilter implements FilterFunction<MqttSensor> {

		private static final long serialVersionUID = 5888231690393233369L;

		@Override
		public boolean filter(MqttSensor value) throws Exception {
			if (value.getKey().f1.equals("COUNTER_TICKETS") && value.getKey().f2.equals(0)) {
				return true;
			}
			return false;
		}
	}

	public static class TrainStationMapper
			implements MapFunction<MqttSensor, Tuple3<Integer, Tuple4<Integer, String, Integer, Integer>, Double>> {

		private static final long serialVersionUID = -5565228597255633611L;

		@Override
		public Tuple3<Integer, Tuple4<Integer, String, Integer, Integer>, Double> map(MqttSensor value)
				throws Exception {
			Integer sensorId = value.getKey().f0;
			String sensorType = value.getKey().f1;
			Integer platformId = value.getKey().f2;
			Integer stationKey = value.getKey().f3;
			Double v = value.getValue();
			return Tuple3.of(stationKey, Tuple4.of(sensorId, sensorType, platformId, stationKey), v);
		}
	}

	public static class AverageAggregator implements
			AggregateFunction<Tuple3<Integer, Tuple4<Integer, String, Integer, Integer>, Double>, Tuple2<Double, Long>, Double> {

		private static final long serialVersionUID = 7233937097358437044L;

		@Override
		public Tuple2<Double, Long> createAccumulator() {
			return new Tuple2<>(0.0, 0L);
		}

		@Override
		public Tuple2<Double, Long> add(Tuple3<Integer, Tuple4<Integer, String, Integer, Integer>, Double> value,
				Tuple2<Double, Long> accumulator) {
			return new Tuple2<>(accumulator.f0 + value.f2, accumulator.f1 + 1L);
		}

		@Override
		public Double getResult(Tuple2<Double, Long> accumulator) {
			return ((double) accumulator.f0) / accumulator.f1;
		}

		@Override
		public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
			return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
		}
	}
}

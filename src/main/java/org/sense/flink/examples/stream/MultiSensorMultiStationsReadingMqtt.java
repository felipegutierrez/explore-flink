package org.sense.flink.examples.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.mqtt.MqttSensorConsumer;

public class MultiSensorMultiStationsReadingMqtt {

	private boolean checkpointEnable = false;
	private long checkpointInterval = 10000;
	private CheckpointingMode checkpointMode = CheckpointingMode.EXACTLY_ONCE;

	public MultiSensorMultiStationsReadingMqtt() throws Exception {

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// obtain execution environment, run this example in "ingestion time"
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		if (checkpointEnable) {
			env.enableCheckpointing(checkpointInterval, checkpointMode);
		}

		DataStream<MqttSensor> streamStation01 = env.addSource(new MqttSensorConsumer("topic-station-01"));
		DataStream<MqttSensor> streamStation02 = env.addSource(new MqttSensorConsumer("topic-station-02"));

		DataStream<MqttSensor> streamStations = streamStation01.union(streamStation02);
		streamStations.print();

		// @formatter:off
		streamStations.filter(new StationPeopleCountFilter())
				.map(new TrainStationMapper())
				.keyBy(0)
				.reduce(new CountPeopleReduce())
				.print();
		// @formatter:on

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("MultiSensorMultiStationsReadingMqtt");
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

	public static class CountPeopleReduce
			implements ReduceFunction<Tuple3<Integer, Tuple4<Integer, String, Integer, Integer>, Double>> {

		private static final long serialVersionUID = -3323622065195829932L;

		@Override
		public Tuple3<Integer, Tuple4<Integer, String, Integer, Integer>, Double> reduce(
				Tuple3<Integer, Tuple4<Integer, String, Integer, Integer>, Double> value1,
				Tuple3<Integer, Tuple4<Integer, String, Integer, Integer>, Double> value2) throws Exception {
			Double sum = value1.f2 + value2.f2;
			return Tuple3.of(value1.f0, Tuple4.of(value1.f1.f0, value1.f1.f1, value1.f1.f2, value1.f1.f3), sum);
		}
	}

	// public static class CountPeopleMapper implements
	// @formatter:off
	/*
	public static class SensorMapper implements MapFunction<MqttTemperature, Tuple2<String, MqttTemperature>> {

		@Override
		public Tuple2<String, MqttTemperature> map(MqttTemperature value) throws Exception {
			String key = "no-room";
			if (value.getId().equals(1) || value.getId().equals(2) || value.getId().equals(3)) {
				key = "room-A";
			} else if (value.getId().equals(4) || value.getId().equals(5) || value.getId().equals(6)) {
				key = "room-B";
			} else if (value.getId().equals(7) || value.getId().equals(8) || value.getId().equals(9)) {
				key = "room-C";
			} else {
				System.err.println("Sensor not defined in any room.");
			}
			return new Tuple2<>(key, value);
		}
	}

	public static class AverageTempMapper
			extends RichFlatMapFunction<Tuple2<String, MqttTemperature>, Tuple2<String, Double>> {

		private static final long serialVersionUID = -4780146677198295204L;
		private ValueState<Tuple2<Integer, Double>> modelState;
		private Integer threshold = 3;

		@Override
		public void open(Configuration parameters) throws Exception {
			ValueStateDescriptor<Tuple2<Integer, Double>> descriptor = new ValueStateDescriptor<>("modelState",
					TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Double.class));
			this.modelState = getRuntimeContext().getState(descriptor);
		}

		@Override
		public void flatMap(Tuple2<String, MqttTemperature> value, Collector<Tuple2<String, Double>> out)
				throws Exception {
			Double temp;
			Integer count;
			if (modelState.value() != null) {
				Tuple2<Integer, Double> state = modelState.value();
				count = state.f0 + 1;
				temp = state.f1 + value.f1.getTemp();
			} else {
				count = 1;
				temp = value.f1.getTemp();
			}
			modelState.update(Tuple2.of(count, temp));

			if (count >= threshold) {
				out.collect(Tuple2.of(value.f0, temp / count));
				modelState.update(Tuple2.of(0, 0.0));
			}
		}
	}
	*/
	// @formatter:on
}

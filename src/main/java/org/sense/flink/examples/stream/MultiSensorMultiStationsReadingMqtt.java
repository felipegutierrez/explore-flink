package org.sense.flink.examples.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
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
				.flatMap(new AveragePeopleOnStationMapper())
				.print();
		
		streamStations.filter(new StationTicketCountFilter())
				.map(new TrainStationMapper())
				.keyBy(0)
				.flatMap(new AverageTicketOnStationMapper())
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

	public static class AveragePeopleOnStationMapper extends
			RichFlatMapFunction<Tuple3<Integer, Tuple5<Integer, String, Integer, String, Integer>, Double>, Tuple2<String, Double>> {

		private static final long serialVersionUID = -5275774398291456293L;
		private ValueState<Tuple2<Integer, Double>> modelState;
		private Integer threshold = 3;

		public void open(Configuration parameters) throws Exception {
			ValueStateDescriptor<Tuple2<Integer, Double>> descriptor = new ValueStateDescriptor<>("modelState",
					TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Double.class));
			this.modelState = getRuntimeContext().getState(descriptor);
		}

		@Override
		public void flatMap(Tuple3<Integer, Tuple5<Integer, String, Integer, String, Integer>, Double> value,
				Collector<Tuple2<String, Double>> out) throws Exception {
			Integer count = 0;
			Double temp;
			if (modelState.value() != null) {
				Tuple2<Integer, Double> state = modelState.value();
				count = state.f0 + 1;
				temp = state.f1 + value.f2;
			} else {
				count = 1;
				temp = value.f2;
			}
			modelState.update(Tuple2.of(count, temp));
			if (count >= threshold) {
				String trainStation = "Train station: " + String.valueOf(value.f0) + " people average: ";
				out.collect(Tuple2.of(trainStation, temp / count));
				modelState.update(Tuple2.of(0, 0.0));
			}
		}
	}

	public static class AverageTicketOnStationMapper extends
			RichFlatMapFunction<Tuple3<Integer, Tuple5<Integer, String, Integer, String, Integer>, Double>, Tuple2<String, Double>> {

		private static final long serialVersionUID = 7414667009552591009L;
		private ValueState<Tuple2<Integer, Double>> modelState;
		private Integer threshold = 3;

		public void open(Configuration parameters) throws Exception {
			ValueStateDescriptor<Tuple2<Integer, Double>> descriptor = new ValueStateDescriptor<>("modelState",
					TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Double.class));
			this.modelState = getRuntimeContext().getState(descriptor);
		}

		@Override
		public void flatMap(Tuple3<Integer, Tuple5<Integer, String, Integer, String, Integer>, Double> value,
				Collector<Tuple2<String, Double>> out) throws Exception {
			Integer count = 0;
			Double temp;
			if (modelState.value() != null) {
				Tuple2<Integer, Double> state = modelState.value();
				count = state.f0 + 1;
				temp = state.f1 + value.f2;
			} else {
				count = 1;
				temp = value.f2;
			}
			modelState.update(Tuple2.of(count, temp));
			if (count >= threshold) {
				String trainStation = "Train station: " + String.valueOf(value.f0) + " ticket average: ";
				out.collect(Tuple2.of(trainStation, temp / count));
				modelState.update(Tuple2.of(0, 0.0));
			}
		}
	}
}

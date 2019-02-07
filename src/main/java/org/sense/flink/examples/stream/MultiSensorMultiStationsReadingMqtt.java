package org.sense.flink.examples.stream;

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
		// DataStream<MqttSensor> streamStation02 = env.addSource(new MqttSensorConsumer("topic-station-02"));

		streamStation01.print();
		// @formatter:off
		/*
		DataStream<Tuple2<String, Double>> averageStreams = temperatureStream01
				.union(temperatureStream02)
				.map(new SensorMatcher())
				.keyBy(0)
				.flatMap(new AverageTempMapper());
		*/
		// averageStreams.print();
		// @formatter:on

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("MultiSensorMultiStationsReadingMqtt");
	}

	// @formatter:off
	/*
	public static class SensorMatcher implements MapFunction<MqttTemperature, Tuple2<String, MqttTemperature>> {

		private static final long serialVersionUID = 7035756567190539683L;

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

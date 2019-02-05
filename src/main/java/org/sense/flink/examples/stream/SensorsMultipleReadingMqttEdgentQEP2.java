package org.sense.flink.examples.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.sense.flink.mqtt.MqttTemperature;
import org.sense.flink.mqtt.TemperatureMqttConsumer;

public class SensorsMultipleReadingMqttEdgentQEP2 {

	private boolean checkpointEnable = false;
	private long checkpointInterval = 10000;
	private CheckpointingMode checkpointMode = CheckpointingMode.EXACTLY_ONCE;

	public SensorsMultipleReadingMqttEdgentQEP2() throws Exception {

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// obtain execution environment, run this example in "ingestion time"
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		if (checkpointEnable) {
			env.enableCheckpointing(checkpointInterval, checkpointMode);
		}

		DataStream<MqttTemperature> temperatureStream01 = env.addSource(new TemperatureMqttConsumer("topic-edgent-01"));
		DataStream<MqttTemperature> temperatureStream02 = env.addSource(new TemperatureMqttConsumer("topic-edgent-02"));
		DataStream<MqttTemperature> temperatureStream03 = env.addSource(new TemperatureMqttConsumer("topic-edgent-03"));

		DataStream<Tuple2<String, Double>> averageStream01 = temperatureStream01.map(new SensorMatcher()).keyBy(0)
				.flatMap(new AverageTempMapper());
		DataStream<Tuple2<String, Double>> averageStream02 = temperatureStream02.map(new SensorMatcher()).keyBy(0)
				.flatMap(new AverageTempMapper());
		DataStream<Tuple2<String, Double>> averageStream03 = temperatureStream03.map(new SensorMatcher()).keyBy(0)
				.flatMap(new AverageTempMapper());

		DataStream<Tuple2<String, Double>> averageStreams = averageStream01.union(averageStream02)
				.union(averageStream03);

		averageStreams.print();

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("SensorsMultipleReadingMqttEdgentQEP");
	}

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
		private MapState<String, Tuple2<Integer, Double>> modelState;
		private Integer threshold = 3;

		@Override
		public void open(Configuration parameters) throws Exception {
			TypeInformation<Tuple2<Integer, Double>> typeInformation = TypeInformation
					.of(new TypeHint<Tuple2<Integer, Double>>() {
					});

			MapStateDescriptor<String, Tuple2<Integer, Double>> descriptor = new MapStateDescriptor<String, Tuple2<Integer, Double>>(
					"modelState", TypeInformation.of(String.class), typeInformation);
			modelState = getRuntimeContext().getMapState(descriptor);
		}

		@Override
		public void flatMap(Tuple2<String, MqttTemperature> value, Collector<Tuple2<String, Double>> out)
				throws Exception {
			Integer count = 0;
			Double temp = 0.0;

			if (modelState.contains(value.f0)) {
				// there is already a value on the state
				count = modelState.get(value.f0).f0 + 1;
				temp = modelState.get(value.f0).f1 + value.f1.getTemp();
				modelState.put(value.f0, Tuple2.of(1, value.f1.getTemp()));
			} else {
				// there is no value on the state
				count = 1;
				temp = value.f1.getTemp();
			}
			modelState.put(value.f0, Tuple2.of(count, temp));

			if (count >= threshold) {
				// only compute the average after the threshold
				out.collect(Tuple2.of(value.f0, temp / count));
				// clear the modelState value in order to compute new values next time
				modelState.put(value.f0, Tuple2.of(0, 0.0));
			}
		}
	}
}

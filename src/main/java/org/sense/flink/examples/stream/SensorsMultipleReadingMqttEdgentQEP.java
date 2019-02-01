package org.sense.flink.examples.stream;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.sense.flink.mqtt.MqttTemperature;
import org.sense.flink.mqtt.TemperatureMqttConsumer;

public class SensorsMultipleReadingMqttEdgentQEP {

	public SensorsMultipleReadingMqttEdgentQEP() throws Exception {

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// obtain execution environment, run this example in "ingestion time"
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<MqttTemperature> temperatureStream01 = env.addSource(new TemperatureMqttConsumer("topic-edgent-01"));
		DataStream<MqttTemperature> temperatureStream02 = env.addSource(new TemperatureMqttConsumer("topic-edgent-02"));
		DataStream<MqttTemperature> temperatureStream03 = env.addSource(new TemperatureMqttConsumer("topic-edgent-03"));
		DataStream<MqttTemperature> temperatureStreams = temperatureStream01.union(temperatureStream02)
				.union(temperatureStream03);

		DataStream<Tuple2<String, Double>> average = temperatureStreams.keyBy(new TemperatureKeySelector())
				.map(new AverageTempMapper());

		average.print();

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("SensorsMultipleReadingMqttEdgentQEP");
	}

	public static class TemperatureKeySelector implements KeySelector<MqttTemperature, Integer> {

		private static final long serialVersionUID = 5905504239899133953L;

		@Override
		public Integer getKey(MqttTemperature value) throws Exception {
			return value.getId();
		}
	}

	public static class AverageTempMapper extends RichMapFunction<MqttTemperature, Tuple2<String, Double>> {

		private static final long serialVersionUID = -5489672634096634902L;
		private MapState<String, Double> averageTemp;

		@Override
		public void open(Configuration parameters) throws Exception {
			averageTemp = getRuntimeContext()
					.getMapState(new MapStateDescriptor<>("average-temperature", String.class, Double.class));
		}

		@Override
		public Tuple2<String, Double> map(MqttTemperature value) throws Exception {
			String key = "no-room";
			Double temp = value.getTemp();

			if (value.getId().equals(1) || value.getId().equals(2) || value.getId().equals(3)) {
				key = "room-A";
			} else if (value.getId().equals(4) || value.getId().equals(5) || value.getId().equals(6)) {
				key = "room-B";
			} else if (value.getId().equals(7) || value.getId().equals(8) || value.getId().equals(9)) {
				key = "room-C";
			} else {
				System.err.println("Sensor not defined in any room.");
			}
			if (averageTemp.contains(key)) {
				temp = (averageTemp.get(key) + value.getTemp()) / 2;
			} else {
				averageTemp.put(key, temp);
			}
			return new Tuple2<String, Double>(key, temp);
		}
	}
}

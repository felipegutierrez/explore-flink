package org.sense.flink.examples.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.udfs.SensorSkewedJoinFunction;
import org.sense.flink.examples.stream.udfs.SensorSkewedJoinedMapFunction;
import org.sense.flink.examples.stream.udfs.StationPlatformKeySelector;
import org.sense.flink.examples.stream.udfs.StationPlatformMapper;
import org.sense.flink.mqtt.CompositeKeyStationPlatform;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.mqtt.MqttSensorConsumer;
import org.sense.flink.mqtt.MqttStringPublisher;

public class MqttSensorDataSkewedJoinDAG {

	private final String topic = "topic-data-skewed-join";

	public static void main(String[] args) throws Exception {
		new MqttSensorDataSkewedJoinDAG("192.168.56.20", "192.168.56.1");
	}

	public MqttSensorDataSkewedJoinDAG(String ipAddressSource01, String ipAddressSink) throws Exception {

		// @formatter:off
		System.out.println("App 14 selected (Complex shuffle with aggregation over a window)");
		System.out.println("Use [./bin/flink run examples/explore-flink.jar 14 " + ipAddressSource01 + " " + ipAddressSink + " -c] to run this program on the Flink standalone-cluster");
		System.out.println("Consuming values from 2 MQTT topics");
		System.out.println("Use 'mosquitto_sub -h " + ipAddressSource01 + " -t " + topic + "' in order to consume data from this job.");
		// @formatter:on

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// obtain execution environment, run this example in "ingestion time"
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		// the period of the emitted markers is 5 milliseconds
		env.getConfig().setLatencyTrackingInterval(5L);

		DataStream<MqttSensor> streamTrainsStation01 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, "topic-station-01-trains"))
				.name(MqttSensorDataSkewedJoinDAG.class.getSimpleName() + "-topic-station-01-trains");
		DataStream<MqttSensor> streamTicketsStation01 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, "topic-station-01-tickets"))
				.name(MqttSensorDataSkewedJoinDAG.class.getSimpleName() + "-topic-station-01-tickets");

		// @formatter:off
		DataStream<Tuple2<CompositeKeyStationPlatform, MqttSensor>> mappedTrainsStation01 = streamTrainsStation01
				.map(new StationPlatformMapper()).name(StationPlatformMapper.class.getSimpleName() + "-trains01")
				.setParallelism(4);
		DataStream<Tuple2<CompositeKeyStationPlatform, MqttSensor>> mappedTicketsStation01 = streamTicketsStation01
				.map(new StationPlatformMapper()).name(StationPlatformMapper.class.getSimpleName() + "-tickets01")
				.setParallelism(4);
		
		mappedTrainsStation01.join(mappedTicketsStation01)
				.where(new StationPlatformKeySelector())
				.equalTo(new StationPlatformKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
				.apply(new SensorSkewedJoinFunction())
				.map(new SensorSkewedJoinedMapFunction()).name(SensorSkewedJoinedMapFunction.class.getSimpleName())
				.setParallelism(4)
				.addSink(new MqttStringPublisher(ipAddressSink, topic)).name(MqttStringPublisher.class.getSimpleName())
				.setParallelism(2)
				;

		// @formatter:on

		System.out.println("........................ ");
		System.out.println("ExecutionPlan: " + env.getExecutionPlan());
		System.out.println("........................ ");

		env.execute(MqttSensorDataSkewedJoinDAG.class.getSimpleName());
	}
}

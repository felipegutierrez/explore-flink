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
	private final String topic_station_01_trains = "topic-station-01-trains";
	private final String topic_station_01_tickets = "topic-station-01-tickets";
	private final String topic_station_02_trains = "topic-station-02-trains";
	private final String topic_station_02_tickets = "topic-station-02-tickets";

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
		// env.getConfig().setLatencyTrackingInterval(5L);

		// Data sources
		DataStream<MqttSensor> streamTrainsStation01 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, topic_station_01_trains))
				.name(MqttSensorConsumer.class.getSimpleName() + "-" + topic_station_01_trains);
		DataStream<MqttSensor> streamTicketsStation01 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, topic_station_01_tickets))
				.name(MqttSensorConsumer.class.getSimpleName() + "-" + topic_station_01_tickets);
		DataStream<MqttSensor> streamTrainsStation02 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, topic_station_02_trains))
				.name(MqttSensorConsumer.class.getSimpleName() + "-" + topic_station_02_trains);
		DataStream<MqttSensor> streamTicketsStation02 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, topic_station_02_tickets))
				.name(MqttSensorConsumer.class.getSimpleName() + "-" + topic_station_02_tickets);

		// union the similar sets
		DataStream<MqttSensor> streamTrainsStations = streamTrainsStation01.union(streamTrainsStation02);
		DataStream<MqttSensor> streamTicketsStations = streamTicketsStation01.union(streamTicketsStation02);

		// @formatter:off
		// map the keys
		DataStream<Tuple2<CompositeKeyStationPlatform, MqttSensor>> mappedTrainsStation = streamTrainsStations
				.map(new StationPlatformMapper()).name(StationPlatformMapper.class.getSimpleName() + "-trains")
				// .setParallelism(4)
				;
		DataStream<Tuple2<CompositeKeyStationPlatform, MqttSensor>> mappedTicketsStation = streamTicketsStations
				.map(new StationPlatformMapper()).name(StationPlatformMapper.class.getSimpleName() + "-tickets")
				// .setParallelism(4)
				;

		mappedTrainsStation.join(mappedTicketsStation)
				.where(new StationPlatformKeySelector())
				.equalTo(new StationPlatformKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
				.apply(new SensorSkewedJoinFunction())
				.map(new SensorSkewedJoinedMapFunction()).name(SensorSkewedJoinedMapFunction.class.getSimpleName())
				// .setParallelism(4)
				.addSink(new MqttStringPublisher(ipAddressSink, topic)).name(MqttStringPublisher.class.getSimpleName())
				// .setParallelism(2)
				;

		// @formatter:on

		System.out.println("........................ ");
		System.out.println("ExecutionPlan: " + env.getExecutionPlan());
		System.out.println("........................ ");

		env.execute(MqttSensorDataSkewedJoinDAG.class.getSimpleName());
	}
}

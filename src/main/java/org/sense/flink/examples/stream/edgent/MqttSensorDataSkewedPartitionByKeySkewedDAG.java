package org.sense.flink.examples.stream.edgent;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.udf.impl.StationPlatformMapper;
import org.sense.flink.examples.stream.udf.impl.StationPlatformSkewedKeyMapper;
import org.sense.flink.examples.stream.udf.impl.StationPlatformSkewedKeyRichWindowFunction;
import org.sense.flink.examples.stream.udf.impl.StationPlatformSkewedKeySelector;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.mqtt.MqttSensorConsumer;
import org.sense.flink.mqtt.MqttStationPlatformPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttSensorDataSkewedPartitionByKeySkewedDAG {

	final static Logger logger = LoggerFactory.getLogger(MqttSensorDataSkewedPartitionByKeySkewedDAG.class);
	private final String topic = "topic-data-skewed-join";
	private final String topic_station_01_trains = "topic-station-01-trains";
	private final String topic_station_01_tickets = "topic-station-01-tickets";
	private final String topic_station_02_trains = "topic-station-02-trains";
	private final String topic_station_02_tickets = "topic-station-02-tickets";

	private final String metricMapper = "StationPlatformMapper";
	private final String metricWindowFunction = "StationPlatformRichWindowFunction";
	private final String metricSkewedMapper = "StationPlatformSkewedMapper";
	private final String metricSinkFunction = "SinkFunction";

	public static void main(String[] args) throws Exception {
		new MqttSensorDataSkewedPartitionByKeySkewedDAG("192.168.56.20", "192.168.56.1");
	}

	public MqttSensorDataSkewedPartitionByKeySkewedDAG(String ipAddressSource01, String ipAddressSink)
			throws Exception {

		System.out.println("App 17 selected (Complex shuffle with aggregation over a window with keyBy skewed)");
		System.out.println("This Flink application fix the skewed key of the App 15");

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// obtain execution environment, run this example in "ingestion time"
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

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

		// @formatter:off
		streamTrainsStation01.union(streamTrainsStation02)
				.union(streamTicketsStation01).union(streamTicketsStation02)
				// map the keys
				.map(new StationPlatformSkewedKeyMapper(metricMapper)).name(metricMapper)
				.keyBy(new StationPlatformSkewedKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
				.apply(new StationPlatformSkewedKeyRichWindowFunction(metricWindowFunction)).name(metricWindowFunction)
				.setParallelism(4)
				.map(new StationPlatformMapper(metricSkewedMapper)).name(metricSkewedMapper)
				.addSink(new MqttStationPlatformPublisher(ipAddressSink, topic)).name(metricSinkFunction)
				;
		// @formatter:on

		System.out.println("........................ ");
		System.out.println("ExecutionPlan: " + env.getExecutionPlan());
		System.out.println("........................ ");

		env.execute(MqttSensorDataSkewedPartitionByKeyDAG.class.getSimpleName());
	}
}

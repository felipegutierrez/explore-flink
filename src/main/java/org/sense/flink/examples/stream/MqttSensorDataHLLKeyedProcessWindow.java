package org.sense.flink.examples.stream;

import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.sense.flink.examples.stream.udf.impl.CompositeKeySensorTypePlatformStationKeyedProcessFunction;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.mqtt.MqttSensorConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttSensorDataHLLKeyedProcessWindow {

	private static final Logger logger = LoggerFactory.getLogger(MqttSensorDataHLLKeyedProcessWindow.class);

	private final String topic_station_01_people = "topic-station-01-people";
	private final String topic_station_01_tickets = "topic-station-01-tickets";
	private final String topic_station_02_people = "topic-station-02-people";
	private final String topic_station_02_tickets = "topic-station-02-tickets";

	private final String metricSensorMapper = "SensorTypeStationPlatformMapper";

	public static void main(String[] args) throws Exception {
		// newMqttSensorDataSkewedCombinerByKeySkewedDAG("192.168.56.20","192.168.56.1");
		new MqttSensorDataHLLKeyedProcessWindow("127.0.0.1", "127.0.0.1");
	}

	public MqttSensorDataHLLKeyedProcessWindow(String ipAddressSource01, String ipAddressSink) throws Exception {

		System.out.println("App 21 selected (Estimate cardinality with HyperLogLog)");
		// @formatter:off
		/**These commands clean all previous metrics related to this application
		sudo curl -XPOST -g 'http://localhost:9090/api/v2/admin/tsdb/delete_series' -d '{"matchers": [{"name": "__name__", "value": "flink_taskmanager_job_task_numBytesOutPerSecond"}]}'
		sudo curl -XPOST -g 'http://localhost:9090/api/v2/admin/tsdb/delete_series' -d '{"matchers": [{"name": "__name__", "value": "flink_taskmanager_job_task_numBytesInLocalPerSecond"}]}'
		sudo curl -XPOST -g 'http://localhost:9090/api/v2/admin/tsdb/delete_series' -d '{"matchers": [{"name": "__name__", "value": "flink_taskmanager_job_task_operator_StationPlatformMapper_meter"}]}'
		sudo curl -XPOST -g 'http://localhost:9090/api/v2/admin/tsdb/delete_series' -d '{"matchers": [{"name": "__name__", "value": "flink_taskmanager_job_task_operator_StationPlatformRichWindowFunction_meter"}]}'
		sudo curl -XPOST -g 'http://localhost:9090/api/v2/admin/tsdb/delete_series' -d '{"matchers": [{"name": "__name__", "value": "flink_taskmanager_job_task_operator_numRecordsOutPerSecond"}]}'
		**/
		// @formatter:on

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Data sources
		DataStream<MqttSensor> streamPeopleStation01 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, topic_station_01_people))
				.name(MqttSensorConsumer.class.getSimpleName() + "-" + topic_station_01_people);
		DataStream<MqttSensor> streamTicketsStation01 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, topic_station_01_tickets))
				.name(MqttSensorConsumer.class.getSimpleName() + "-" + topic_station_01_tickets);
		DataStream<MqttSensor> streamPeopleStation02 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, topic_station_02_people))
				.name(MqttSensorConsumer.class.getSimpleName() + "-" + topic_station_02_people);
		DataStream<MqttSensor> streamTicketsStation02 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, topic_station_02_tickets))
				.name(MqttSensorConsumer.class.getSimpleName() + "-" + topic_station_02_tickets);

		DataStream<MqttSensor> streamStation01 = streamPeopleStation01.union(streamTicketsStation01);
		DataStream<MqttSensor> streamStation02 = streamPeopleStation02.union(streamTicketsStation02);

		// @formatter:off
		streamStation01.union(streamStation02)
				// .map(new SensorTypePlatformStationMapper(metricSensorMapper)).name(metricSensorMapper)
				.keyBy(new NullByteKeySelector<MqttSensor>())
				.process(new CompositeKeySensorTypePlatformStationKeyedProcessFunction(5 * 1000))
				.print();
		// @formatter:on

		System.out.println("........................ ");
		System.out.println("ExecutionPlan: " + env.getExecutionPlan());
		System.out.println("........................ ");

		env.execute(MqttSensorDataHLLKeyedProcessWindow.class.getSimpleName());
	}
}

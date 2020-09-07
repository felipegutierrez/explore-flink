package org.sense.flink.examples.stream.table;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttSensorDataAverageTableAPI {
	private static final Logger logger = LoggerFactory.getLogger(MqttSensorDataAverageTableAPI.class);
	private final String topic_station_01_people = "topic-station-01-people";
	private final String topic_station_01_tickets = "topic-station-01-tickets";
	private final String topic_station_02_people = "topic-station-02-people";
	private final String topic_station_02_tickets = "topic-station-02-tickets";

	public static void main(String[] args) throws Exception {
		// new MqttSensorDataAverageTableAPI("192.168.56.20","192.168.56.1");
		new MqttSensorDataAverageTableAPI("127.0.0.1", "127.0.0.1");
	}

	public MqttSensorDataAverageTableAPI(String ipAddressSource01, String ipAddressSink) throws Exception {
		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Create Stream table environment
		// StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// obtain query configuration from TableEnvironment
		// StreamQueryConfig qConfig = tableEnv.queryConfig();
		// set query parameters
		// qConfig.withIdleStateRetentionTime(Time.minutes(30), Time.hours(2));

		// @formatter:off
		// Register Data Source Stream tables in the table environment
//		tableEnv.registerTableSource("PeopleStation01", new MqttSensorTableSource(ipAddressSource01, topic_station_01_people));

		// Query
//		Table result = tableEnv.scan("PeopleStation01")
//				.window(Tumble.over("5.seconds").on("eventTime").as("eventTimeWindow"))
//				.groupBy("eventTimeWindow, stationId, platformType, sensorType")
//				.select("eventTimeWindow.end as second, stationId, platformType, sensorType, value.avg as avgValue")
//				.filter("platformType = 'CIT'")
//				;
//
//		tableEnv.toAppendStream(result, Row.class).print();
		// @formatter:on

		// result.printSchema();
		// System.out.println("Execution plan ........................ ");
		// System.out.println(env.getExecutionPlan());
		// System.out.println("Plan explaination ........................ ");
		// System.out.println(tableEnv.explain(result));
		// System.out.println("........................ ");

		env.execute("MqttSensorDataAverageTableAPI");
	}
}

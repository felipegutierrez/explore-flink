package org.sense.flink.examples.stream.table;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.sense.flink.mqtt.MqttSensorTupleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttSensorDataJoinTableAPI {
	private static final Logger logger = LoggerFactory.getLogger(MqttSensorDataJoinTableAPI.class);
	private final String topic_station_01_people = "topic-station-01-people";
	private final String topic_station_01_tickets = "topic-station-01-tickets";
	private final String topic_station_02_people = "topic-station-02-people";
	private final String topic_station_02_tickets = "topic-station-02-tickets";

	public static void main(String[] args) throws Exception {
		// new MqttSensorDataJoinTableAPI("192.168.56.20","192.168.56.1");
		new MqttSensorDataJoinTableAPI("127.0.0.1", "127.0.0.1");
	}

	public MqttSensorDataJoinTableAPI(String ipAddressSource01, String ipAddressSink) throws Exception {
		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// obtain query configuration from TableEnvironment
		StreamQueryConfig qConfig = tableEnv.queryConfig();
		// set query parameters
		qConfig.withIdleStateRetentionTime(Time.minutes(30), Time.hours(2));

		// @formatter:off
		// Data sources
		DataStream<Tuple8<Integer, String, Integer, String, Integer, Long, Double, String>> streamPeopleStation01 = env
				.addSource(new MqttSensorTupleConsumer(ipAddressSource01, topic_station_01_people));
		DataStream<Tuple8<Integer, String, Integer, String, Integer, Long, Double, String>> streamTicketsStation01 = env
				.addSource(new MqttSensorTupleConsumer(ipAddressSource01, topic_station_01_tickets));
		
		// Data tables
		Table tablePeopleStation01 = tableEnv
				.fromDataStream(streamPeopleStation01, "sensorIdP1, sensorTypeP1, platformIdP1, platformTypeP1, stationIdP1, timestampP1, valueP1, tripP1");
		Table tableTicketsStation01 = tableEnv
				.fromDataStream(streamTicketsStation01, "sensorIdT1, sensorTypeT1, platformIdT1, platformTypeT1, stationIdT1, timestampT1, valueT1, tripT1");
		
		// Query
		Table result = tablePeopleStation01.join(tableTicketsStation01)
				.where("stationIdP1 === stationIdT1 && platformIdP1 === platformIdT1")
				.select("stationIdP1, platformIdP1, sensorTypeP1, valueP1, sensorTypeT1, valueT1")
				;
		
		tableEnv.toAppendStream(result, Row.class).print();
		// @formatter:on

		System.out.println("Execution plan ........................ ");
		System.out.println(env.getExecutionPlan());
		System.out.println("Plan explaination ........................ ");
		System.out.println(tableEnv.explain(result));
		System.out.println("........................ ");

		env.execute("MqttSensorDataJoinTableAPI");
	}
}

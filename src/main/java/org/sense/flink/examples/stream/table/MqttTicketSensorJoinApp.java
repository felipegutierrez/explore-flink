package org.sense.flink.examples.stream.table;

import static org.sense.flink.util.SensorColumn.EVENTTIME;
import static org.sense.flink.util.SensorColumn.LEFT;
import static org.sense.flink.util.SensorColumn.PLATFORM_ID;
import static org.sense.flink.util.SensorColumn.PLATFORM_TYPE;
import static org.sense.flink.util.SensorColumn.RIGHT;
import static org.sense.flink.util.SensorColumn.SENSOR_TYPE;
import static org.sense.flink.util.SensorColumn.STATION_ID;
import static org.sense.flink.util.SensorColumn.TRIP;
import static org.sense.flink.util.SensorColumn.VALUE;
import static org.sense.flink.util.SensorTopics.TOPIC_STATION_01_PLAT_01_TICKETS;
import static org.sense.flink.util.SensorTopics.TOPIC_STATION_01_PLAT_02_TICKETS;

import org.apache.calcite.tools.RuleSets;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.calcite.CalciteConfigBuilder;
import org.apache.flink.types.Row;
import org.sense.calcite.rules.MyFilterIntoJoinRule;
import org.sense.calcite.rules.MyFilterJoinRule;
import org.sense.flink.mqtt.MqttSensorTableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttTicketSensorJoinApp {
	private static final Logger logger = LoggerFactory.getLogger(MqttTicketSensorJoinApp.class);
	private static final String TICKETS_STATION_01_PLATFORM_01 = "TicketsStation01Plat01";
	private static final String TICKETS_STATION_01_PLATFORM_02 = "TicketsStation01Plat02";

	public static void main(String[] args) throws Exception {
		// new MqttSensorDataAverageTableAPI("192.168.56.20","192.168.56.1");
		new MqttTicketSensorJoinApp("127.0.0.1", "127.0.0.1");
	}

	public MqttTicketSensorJoinApp(String ipAddressSource01, String ipAddressSink) throws Exception {
		// @formatter:off
		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Create Stream table environment
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// Calcite configuration file to change the query execution plan
		CalciteConfig cc = new CalciteConfigBuilder()
				// .addLogicalOptRuleSet(RuleSets.ofList(MyFilterIntoJoinRule.INSTANCE))
				.addLogicalOptRuleSet(RuleSets.ofList(MyFilterJoinRule.FILTER_ON_JOIN))
				.build();
		//tableEnv.getConfig().setCalciteConfig(cc);

		// obtain query configuration from TableEnvironment
		//StreamQueryConfig qConfig = tableEnv.queryConfig();
		// set query parameters
		//qConfig.withIdleStateRetentionTime(Time.minutes(30), Time.hours(2));

		// @formatter:off
		// Register Data Source Stream tables in the table environment
		tableEnv.registerTableSource(TICKETS_STATION_01_PLATFORM_01, 
				new MqttSensorTableSource(ipAddressSource01, TOPIC_STATION_01_PLAT_01_TICKETS, LEFT));
		tableEnv.registerTableSource(TICKETS_STATION_01_PLATFORM_02, 
				new MqttSensorTableSource(ipAddressSource01, TOPIC_STATION_01_PLAT_02_TICKETS, RIGHT));

		Table left = tableEnv.scan(TICKETS_STATION_01_PLATFORM_01);
		Table right = tableEnv.scan(TICKETS_STATION_01_PLATFORM_02);
		// Query
		String whereClause = LEFT + VALUE + " = " + RIGHT + VALUE + " && " + 
				LEFT + EVENTTIME + " >= " + RIGHT + EVENTTIME + " && " + 
				LEFT + EVENTTIME + " < " + RIGHT + EVENTTIME + " + 10.seconds";
		String selectClause = LEFT + STATION_ID + ", " + LEFT + SENSOR_TYPE + ", " + 
				LEFT + PLATFORM_TYPE + ", " + LEFT + PLATFORM_ID + ", " + RIGHT + PLATFORM_ID + ", " + 
				LEFT + VALUE + ", " + LEFT + TRIP + ", " + RIGHT + VALUE + ", " + RIGHT + TRIP;
		Table result = left.join(right).where(whereClause).select(selectClause);

		tableEnv.toAppendStream(result, Row.class).print();

		result.printSchema();
		System.out.println("Execution plan ........................ ");
		System.out.println(env.getExecutionPlan());
		System.out.println("Plan explaination ........................ ");
		System.out.println(tableEnv.explain(result));
		System.out.println("........................ ");
		// @formatter:on

		env.execute("MqttTicketSensorJoinApp");
		// @formatter:on
	}
}

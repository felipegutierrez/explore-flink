package org.sense.flink.examples.stream.table;

import static org.sense.flink.util.SensorTopics.*;
import static org.sense.flink.util.SensorColumn.*;

import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.tools.RuleSets;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.calcite.CalciteConfig;
import org.apache.flink.table.calcite.CalciteConfigBuilder;
import org.apache.flink.table.plan.rules.datastream.DataStreamRetractionRules;
import org.apache.flink.types.Row;
import org.sense.calcite.rules.MyDataStreamRule;
import org.sense.calcite.rules.MyFilterReduceExpressionRule;
import org.sense.flink.mqtt.MqttSensorTableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloWorldCalcitePlanTableAPI {
	private static final Logger logger = LoggerFactory.getLogger(HelloWorldCalcitePlanTableAPI.class);
	private static final String TICKETS_STATION_01_PLATFORM_01 = "TicketsStation01Plat01";

	public static void main(String[] args) throws Exception {
		new HelloWorldCalcitePlanTableAPI("127.0.0.1", "127.0.0.1");
	}

	public HelloWorldCalcitePlanTableAPI(String ipAddressSource01, String ipAddressSink) throws Exception {
		// @formatter:off
		/*
		 * CalciteConfig cc = new CalciteConfigBuilder()
		 * .addNormRuleSet(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
		 * .replaceLogicalOptRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
		 * .replacePhysicalOptRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
		 * .replaceDecoRuleSet(RuleSets.ofList(DataStreamRetractionRules.
		 * DEFAULT_RETRACTION_INSTANCE())).build();
		 * 
		 * assertFalse(cc.replacesNormRuleSet());
		 * assertTrue(cc.getNormRuleSet().isDefined());
		 * 
		 * assertTrue(cc.replacesLogicalOptRuleSet());
		 * assertTrue(cc.getLogicalOptRuleSet().isDefined());
		 * 
		 * assertTrue(cc.replacesPhysicalOptRuleSet());
		 * assertTrue(cc.getPhysicalOptRuleSet().isDefined());
		 * 
		 * assertTrue(cc.replacesDecoRuleSet());
		 * assertTrue(cc.getDecoRuleSet().isDefined());
		 */

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, tableConfig);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// Calcite configuration file to change the query execution plan
		// CalciteConfig cc = tableEnv.getConfig().getCalciteConfig();
		CalciteConfig cc = new CalciteConfigBuilder()
				// .addNormRuleSet(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
				.addNormRuleSet(RuleSets.ofList(MyFilterReduceExpressionRule.FILTER_INSTANCE))
			    // .replaceLogicalOptRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
			    // .replacePhysicalOptRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
			    // .replaceDecoRuleSet(RuleSets.ofList(DataStreamRetractionRules.DEFAULT_RETRACTION_INSTANCE))
				.replaceDecoRuleSet(RuleSets.ofList(MyDataStreamRule.INSTANCE))
				.build();
		tableEnv.getConfig().setCalciteConfig(cc);

		// obtain query configuration from TableEnvironment
		StreamQueryConfig qConfig = tableEnv.queryConfig();
		qConfig.withIdleStateRetentionTime(Time.minutes(30), Time.hours(2));

		// Register Data Source Stream tables in the table environment
		tableEnv.registerTableSource(TICKETS_STATION_01_PLATFORM_01,
				new MqttSensorTableSource(ipAddressSource01, TOPIC_STATION_01_PLAT_01_TICKETS));
		Table result = tableEnv.scan(TICKETS_STATION_01_PLATFORM_01)
				.filter(VALUE + " >= 50 && " + VALUE + " <= 100 && " + VALUE + " >= 50")
				;
		tableEnv.toAppendStream(result, Row.class).print();

		result.printSchema();
		System.out.println("Execution plan ........................ ");
		System.out.println(env.getExecutionPlan());
		System.out.println("Plan explaination ........................ ");
		System.out.println(tableEnv.explain(result));
		System.out.println("........................ ");
		System.out.println("NormRuleSet: " + cc.getNormRuleSet().isDefined());
		System.out.println("LogicalOptRuleSet: " + cc.getLogicalOptRuleSet().isDefined());
		System.out.println("PhysicalOptRuleSet: " + cc.getPhysicalOptRuleSet().isDefined());
		System.out.println("DecoRuleSet: " + cc.getDecoRuleSet().isDefined());
		// @formatter:on

		env.execute("HelloWorldCalcitePlanTableAPI");
	}
}

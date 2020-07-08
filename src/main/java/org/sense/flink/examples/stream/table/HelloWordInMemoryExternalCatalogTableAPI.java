package org.sense.flink.examples.stream.table;

import java.util.Collections;
import java.util.Map;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloWordInMemoryExternalCatalogTableAPI {
	private static final Logger logger = LoggerFactory.getLogger(HelloWordInMemoryExternalCatalogTableAPI.class);
	private final String databaseName = "db1";

	public static void main(String[] args) throws Exception {
		new HelloWordInMemoryExternalCatalogTableAPI();
	}

	public HelloWordInMemoryExternalCatalogTableAPI() throws Exception {
		// InMemoryExternalCatalog catalog = new InMemoryExternalCatalog(databaseName);
		// assertTrue(catalog.listTables().isEmpty());
		// catalog.createTable("t1", createTableInstance(), false);
		// List<String> tables = catalog.listTables();
		// assertEquals(1, tables.size());
		// assertEquals("t1", tables.get(0));

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// Create Stream table environment
		// StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		// obtain query configuration from TableEnvironment
		// StreamQueryConfig qConfig = tableEnv.queryConfig();
		// set query parameters
		// qConfig.withIdleStateRetentionTime(Time.minutes(30), Time.hours(2));

		// Register Data Source Stream tables in the table environment
		// ExternalCatalogTable t = catalog.getTable("t1");
		// tableEnv.registerTableSource(name, tableSource);
		// tableEnv.registerTableSink(name, configuredSink);
	}

	// private ExternalCatalogTable createTableInstance() {
	// TestConnectorDesc connDesc = new TestConnectorDesc("test", 1, false);
	// @formatter:off
	// Schema schemaDesc = new Schema()
	// 		.field("first", BasicTypeInfo.STRING_TYPE_INFO)
	// 		.field("second", BasicTypeInfo.INT_TYPE_INFO);
	// return new ExternalCatalogTableBuilder(connDesc)
	// 		.withSchema(schemaDesc)
	// 		.asTableSource();
		// @formatter:on
	// }

	public static class TestConnectorDesc extends ConnectorDescriptor {
		public TestConnectorDesc(String type, int version, boolean formatNeeded) {
			super(type, version, formatNeeded);
		}

		@Override
		protected Map<String, String> toConnectorProperties() {
			return Collections.emptyMap();
		}
	}
}

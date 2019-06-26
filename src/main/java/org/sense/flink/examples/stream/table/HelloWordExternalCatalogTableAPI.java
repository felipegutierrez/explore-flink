package org.sense.flink.examples.stream.table;

import static org.junit.Assert.assertTrue;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.ExternalCatalogTableBuilder;
import org.apache.flink.table.catalog.InMemoryExternalCatalog;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloWordExternalCatalogTableAPI {
	private static final Logger logger = LoggerFactory.getLogger(HelloWordExternalCatalogTableAPI.class);
	private final String externalCatalog01 = "resources/externalCatalog01.dbc";
	private final String inMemoryCatalogHLL = "inMemoryCatalogHLL";
	private final String inMemoryTableHLL = "inMemoryTableHLL";

	public static void main(String[] args) throws Exception {
		new HelloWordExternalCatalogTableAPI();
	}

	public HelloWordExternalCatalogTableAPI() throws Exception {
		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// change the current calcite config plan
		// CalciteConfigBuilder ccb = new CalciteConfigBuilder();
		// RuleSet ruleSets = RuleSets.ofList(FilterMergeRule.INSTANCE);
		// ccb.addLogicalOptRuleSet(ruleSets);
		// TableConfig tableConfig = new TableConfig();
		// tableConfig.setCalciteConfig(ccb.build());

		// Create Stream table environment
		// StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,
		// tableConfig);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// register the ExternalCatalog on Table environment
		tableEnv.registerExternalCatalog(inMemoryCatalogHLL, getExternalCatalog());

		Table result = tableEnv.scan(inMemoryCatalogHLL, inMemoryTableHLL);
		result.printSchema();

		System.out.println("Execution plan ........................ ");
		System.out.println(env.getExecutionPlan());
		System.out.println("Plan explaination ........................ ");
		System.out.println(tableEnv.explain(result));
		System.out.println("........................ ");

		env.execute("HelloWordExternalCatalogTableAPI");
	}

	private ExternalCatalog getExternalCatalog() {
		// create an in-memory external catalog
		InMemoryExternalCatalog catalog = new InMemoryExternalCatalog(externalCatalog01);
		assertTrue(catalog.listTables().isEmpty());
		assertTrue(catalog.listSubCatalogs().isEmpty());
		// catalog.createSubCatalog(inMemoryCatalogHLL, catalog, false);
		// catalog.getSubCatalog(inMemoryCatalogHLL);
		// catalog.getTable("").
		catalog.createTable(inMemoryTableHLL, getExternalCatalogTable(), false);

		// catalog.createTable(inMemoryTableHLL, catalog.getTable(inMemoryTableHLL),
		// false);
		return catalog;
	}

	private ExternalCatalogTable getExternalCatalogTable() {
		// @formatter:off
		FileSystem connectorDescriptor = new FileSystem().path("file:/tmp/file-test.txt");
		// FileSystem connectorDescriptor = new FileSystem().path("resources/externalCatalog01.csv");
		// FormatDescriptor csvDesc = new Csv().field("a", "string").field("b",
		// "string").field("c", "string").fieldDelimiter("\t");
		// ExternalCatalogTable t1 =
		// ExternalCatalogTable.builder(connDescIn).withFormat(csvDesc).withSchema(schemaDesc).asTableSource();
		// format
		OldCsv csv = new OldCsv();
		csv.field("table", BasicTypeInfo.STRING_TYPE_INFO);
		csv.field("column", BasicTypeInfo.STRING_TYPE_INFO);
		csv.field("count", BasicTypeInfo.INT_TYPE_INFO);
		csv.fieldDelimiter(",");
		// schema
		Schema schema = new Schema();
		schema.field("table", BasicTypeInfo.STRING_TYPE_INFO);
		schema.field("column", BasicTypeInfo.STRING_TYPE_INFO);
		schema.field("count", BasicTypeInfo.INT_TYPE_INFO);

		return new ExternalCatalogTableBuilder(connectorDescriptor)
				.withSchema(schema)
				.withFormat(csv)
				.asTableSource();
		// @formatter:on
	}
}

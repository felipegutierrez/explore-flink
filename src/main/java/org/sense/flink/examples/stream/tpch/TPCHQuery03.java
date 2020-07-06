package org.sense.flink.examples.stream.tpch;

import static org.sense.flink.util.MetricLabels.OPERATOR_SINK;
import static org.sense.flink.util.MetricLabels.ROCKSDB_STATE_DIR_NFS;
import static org.sense.flink.util.SinkOutputs.PARAMETER_OUTPUT_FILE;
import static org.sense.flink.util.SinkOutputs.PARAMETER_OUTPUT_LOG;
import static org.sense.flink.util.SinkOutputs.PARAMETER_OUTPUT_MQTT;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.sense.flink.examples.stream.tpch.pojo.Order;
import org.sense.flink.examples.stream.tpch.pojo.ShippingPriorityItem;
import org.sense.flink.examples.stream.tpch.udf.OrderCustomerKeySelector;
import org.sense.flink.examples.stream.tpch.udf.OrderKeyedByCustomerProcessFunction;
import org.sense.flink.examples.stream.tpch.udf.OrderTimestampAndWatermarkAssigner;
import org.sense.flink.examples.stream.tpch.udf.OrdersSource;
import org.sense.flink.examples.stream.tpch.udf.ShippingPriority3KeySelector;
import org.sense.flink.examples.stream.tpch.udf.ShippingPriorityItemMap;
import org.sense.flink.examples.stream.tpch.udf.ShippingPriorityKeySelector;
import org.sense.flink.examples.stream.tpch.udf.ShippingPriorityKeyedProcessFunction;
import org.sense.flink.examples.stream.tpch.udf.SumShippingPriorityItem;
import org.sense.flink.mqtt.MqttStringPublisher;

/**
 * Implementation of the TPC-H Benchmark query 03 also available at:
 * 
 * https://github.com/apache/flink/blob/master/flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java/relational/TPCHQuery3.java
 * 
 * The original query can be found at:
 * 
 * https://docs.deistercloud.com/content/Databases.30/TPCH%20Benchmark.90/Sample%20querys.20.xml
 * 
 * <p>
 * This program implements the following SQL equivalent:
 *
 * <p>
 * 
 * <pre>
 * {@code
 * SELECT
 *      l_orderkey,
 *      SUM(l_extendedprice*(1-l_discount)) AS revenue,
 *      o_orderdate,
 *      o_shippriority
 * FROM customer,
 *      orders,
 *      lineitem
 * WHERE
 *      c_mktsegment = '[SEGMENT]'
 *      AND c_custkey = o_custkey
 *      AND l_orderkey = o_orderkey
 *      AND o_orderdate < date '[DATE]'
 *      AND l_shipdate > date '[DATE]'
 * GROUP BY
 *      l_orderkey,
 *      o_orderdate,
 *      o_shippriority;
 * }
 * </pre>
 *
 * <p>
 * Compared to the original TPC-H query this version does not sort the result by
 * revenue and orderdate.
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class TPCHQuery03 {

	private final String topic = "topic-tpch-query-03";

	public static void main(String[] args) throws Exception {
		new TPCHQuery03();
	}

	public TPCHQuery03() throws Exception {
		this("file:///tmp/flink/state", PARAMETER_OUTPUT_LOG, "127.0.0.1", false, false);
	}

	public TPCHQuery03(String output, String ipAddressSink, boolean disableOperatorChaining, boolean pinningPolicy)
			throws Exception {
		this(ROCKSDB_STATE_DIR_NFS, output, ipAddressSink, disableOperatorChaining, pinningPolicy);
	}

	public TPCHQuery03(String stateDir, String output, String ipAddressSink, boolean disableOperatorChaining,
			boolean pinningPolicy) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStateBackend(new RocksDBStateBackend(stateDir, true));
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		if (disableOperatorChaining) {
			env.disableOperatorChaining();
		}

		// @formatter:off
		DataStream<Order> orders = env
				.addSource(new OrdersSource()).name(OrdersSource.class.getSimpleName()).uid(OrdersSource.class.getSimpleName())
				.assignTimestampsAndWatermarks(new OrderTimestampAndWatermarkAssigner());

		// Filter market segment "AUTOMOBILE"
		// customers = customers.filter(new CustomerFilter());

		// Filter all Orders with o_orderdate < 12.03.1995
		// orders = orders.filter(new OrderFilter());

		// Join customers with orders and package them into a ShippingPriorityItem
		DataStream<ShippingPriorityItem> shippingPriorityStream01 = orders
				.keyBy(new OrderCustomerKeySelector())
				.process(new OrderKeyedByCustomerProcessFunction(pinningPolicy)).name(OrderKeyedByCustomerProcessFunction.class.getSimpleName()).uid(OrderKeyedByCustomerProcessFunction.class.getSimpleName());

		// Join the last join result with Lineitems
		DataStream<ShippingPriorityItem> shippingPriorityStream02 = shippingPriorityStream01
				.keyBy(new ShippingPriorityKeySelector())
				.process(new ShippingPriorityKeyedProcessFunction(pinningPolicy)).name(ShippingPriorityKeyedProcessFunction.class.getSimpleName()).uid(ShippingPriorityKeyedProcessFunction.class.getSimpleName());

		// Group by l_orderkey, o_orderdate and o_shippriority and compute revenue sum
		DataStream<ShippingPriorityItem> shippingPriorityStream03 = shippingPriorityStream02
				.keyBy(new ShippingPriority3KeySelector())
				.reduce(new SumShippingPriorityItem(pinningPolicy)).name(SumShippingPriorityItem.class.getSimpleName()).uid(SumShippingPriorityItem.class.getSimpleName());

		// emit result
		if (output.equalsIgnoreCase(PARAMETER_OUTPUT_MQTT)) {
			shippingPriorityStream03
				.map(new ShippingPriorityItemMap(pinningPolicy)).name(ShippingPriorityItemMap.class.getSimpleName()).uid(ShippingPriorityItemMap.class.getSimpleName())
				.addSink(new MqttStringPublisher(ipAddressSink, topic, pinningPolicy)).name(OPERATOR_SINK).uid(OPERATOR_SINK);
		} else if (output.equalsIgnoreCase(PARAMETER_OUTPUT_LOG)) {
			shippingPriorityStream03.print().name(OPERATOR_SINK).uid(OPERATOR_SINK);
		} else if (output.equalsIgnoreCase(PARAMETER_OUTPUT_FILE)) {
			shippingPriorityStream03.print().name(OPERATOR_SINK).uid(OPERATOR_SINK);
		} else {
			System.out.println("discarding output");
		}
		// @formatter:on

		System.out.println("Stream job: " + TPCHQuery03.class.getSimpleName());
		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(TPCHQuery03.class.getSimpleName());
	}
}

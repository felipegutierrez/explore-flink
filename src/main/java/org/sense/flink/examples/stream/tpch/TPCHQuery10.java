package org.sense.flink.examples.stream.tpch;

import static org.sense.flink.util.MetricLabels.OPERATOR_REDUCER;
import static org.sense.flink.util.MetricLabels.OPERATOR_SINK;
import static org.sense.flink.util.SinkOutputs.PARAMETER_OUTPUT_FILE;
import static org.sense.flink.util.SinkOutputs.PARAMETER_OUTPUT_LOG;
import static org.sense.flink.util.SinkOutputs.PARAMETER_OUTPUT_MQTT;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.tpch.pojo.Order;
import org.sense.flink.examples.stream.tpch.udf.JoinCustomerWithRevenueKeyedProcessFunction;
import org.sense.flink.examples.stream.tpch.udf.OrderKeySelector;
import org.sense.flink.examples.stream.tpch.udf.OrderTimestampAndWatermarkAssigner;
import org.sense.flink.examples.stream.tpch.udf.OrdersSource;
import org.sense.flink.examples.stream.tpch.udf.RevenueByCustomerProcessWindow;
import org.sense.flink.examples.stream.tpch.udf.Tuple6ToStringMap;
import org.sense.flink.mqtt.MqttStringPublisher;

/**
 * 
 * Implementation of the TPC-H Benchmark query 03 also available at:
 * 
 * https://github.com/apache/flink/blob/master/flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java/relational/TPCHQuery10.java
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
 *        c_custkey,
 *        c_name,
 *        c_address,
 *        n_name,
 *        c_acctbal
 *        SUM(l_extendedprice * (1 - l_discount)) AS revenue,
 * FROM
 *        customer,
 *        orders,
 *        lineitem,
 *        nation
 * WHERE
 *        c_custkey = o_custkey
 *        AND l_orderkey = o_orderkey
 *        AND YEAR(o_orderdate) > '1990'
 *        AND l_returnflag = 'R'
 *        AND c_nationkey = n_nationkey
 * GROUP BY
 *        c_custkey,
 *        c_name,
 *        c_acctbal,
 *        n_name,
 *        c_address
 * }
 * </pre>
 *
 * <p>
 * Compared to the original TPC-H query this version does not print c_phone and
 * c_comment, only filters by years greater than 1990 instead of a period of 3
 * months, and does not sort the result by revenue.
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class TPCHQuery10 {
	private final String topic = "topic-tpch-query-10";

	public static void main(String[] args) throws Exception {
		new TPCHQuery10();
	}

	public TPCHQuery10() throws Exception {
		this(PARAMETER_OUTPUT_LOG, "127.0.0.1", false, false);
	}

	public TPCHQuery10(String output, String ipAddressSink, boolean disableOperatorChaining, boolean pinningPolicy)
			throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStateBackend(new RocksDBStateBackend("file:///tmp/flink/state", true));
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		if (disableOperatorChaining) {
			env.disableOperatorChaining();
		}

		// @formatter:off
		DataStream<Order> orders = env
				.addSource(new OrdersSource()).name(OrdersSource.class.getSimpleName()).uid(OrdersSource.class.getSimpleName())
				.assignTimestampsAndWatermarks(new OrderTimestampAndWatermarkAssigner());

		// orders filtered by year: (orderkey, custkey)

		// join orders with lineitems: (custkey, revenue)
		DataStream<Tuple2<Integer, Double>> revenueByCustomer = orders
				.keyBy(new OrderKeySelector())
				.timeWindow(Time.seconds(10))
				.process(new RevenueByCustomerProcessWindow(pinningPolicy)).name(RevenueByCustomerProcessWindow.class.getSimpleName()).uid(RevenueByCustomerProcessWindow.class.getSimpleName());

		// sum the revenue by customers
		DataStream<Tuple2<Integer, Double>> revenueByCustomerSum = revenueByCustomer
				.keyBy(0)
				.sum(1).name(OPERATOR_REDUCER).uid(OPERATOR_REDUCER);

		// join customer with nation (custkey, name, address, nationname, acctbal)
		// join customer (with nation) with revenue (custkey, name, address, nationname, acctbal, revenue)
		DataStream<Tuple6<Integer, String, String, String, Double, Double>> result = revenueByCustomerSum
				.keyBy(0)
				.process(new JoinCustomerWithRevenueKeyedProcessFunction(pinningPolicy));

		// emit result
		if (output.equalsIgnoreCase(PARAMETER_OUTPUT_MQTT)) {
			result
			.map(new Tuple6ToStringMap(pinningPolicy)).name(Tuple6ToStringMap.class.getSimpleName()).uid(Tuple6ToStringMap.class.getSimpleName())
			.addSink(new MqttStringPublisher(ipAddressSink, topic, pinningPolicy)).name(OPERATOR_SINK).uid(OPERATOR_SINK);
		} else if (output.equalsIgnoreCase(PARAMETER_OUTPUT_LOG)) {
			result.print().name(OPERATOR_SINK).uid(OPERATOR_SINK);
		} else if (output.equalsIgnoreCase(PARAMETER_OUTPUT_FILE)) {
			result.print().name(OPERATOR_SINK).uid(OPERATOR_SINK);
		} else {
			System.out.println("discarding output");
		}
		// @formatter:on

		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(TPCHQuery10.class.getSimpleName());
	}
}

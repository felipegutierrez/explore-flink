package org.sense.flink.examples.stream.tpch;

import static org.sense.flink.util.SinkOutputs.PARAMETER_OUTPUT_LOG;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.tpch.pojo.Order;
import org.sense.flink.examples.stream.tpch.udf.OrderKeySelector;
import org.sense.flink.examples.stream.tpch.udf.OrderTimestampAndWatermarkAssigner;
import org.sense.flink.examples.stream.tpch.udf.OrdersSource;
import org.sense.flink.examples.stream.tpch.udf.RevenueByCustomerProcessWindow;

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

		DataStream<Order> orders = env.addSource(new OrdersSource()).name(OrdersSource.class.getSimpleName())
				.uid(OrdersSource.class.getSimpleName())
				.assignTimestampsAndWatermarks(new OrderTimestampAndWatermarkAssigner());

		// orders filtered by year: (orderkey, custkey)

		// join orders with lineitems: (custkey, revenue)
		DataStream<Tuple2<Integer, Double>> revenueByCustomer = orders.keyBy(new OrderKeySelector())
				.timeWindow(Time.seconds(10)).process(new RevenueByCustomerProcessWindow(pinningPolicy))
				.name(RevenueByCustomerProcessWindow.class.getSimpleName())
				.uid(RevenueByCustomerProcessWindow.class.getSimpleName());

		revenueByCustomer.print();

		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(TPCHQuery10.class.getSimpleName());
	}
}

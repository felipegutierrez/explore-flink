package org.sense.flink.examples.stream.tpch;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.sense.flink.examples.stream.tpch.pojo.Order;
import org.sense.flink.examples.stream.tpch.pojo.ShippingPriorityItem;
import org.sense.flink.examples.stream.tpch.udf.*;
import org.sense.flink.mqtt.MqttStringPublisher;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.sense.flink.util.MetricLabels.OPERATOR_SINK;
import static org.sense.flink.util.SinkOutputs.*;

/**
 * Implementation of the TPC-H Benchmark query 03 also available at:
 * <p>
 * https://github.com/apache/flink/blob/master/flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java/relational/TPCHQuery3.java
 * <p>
 * The original query can be found at:
 * <p>
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
 * @author Felipe Oliveira Gutierrez
 */
public class TPCHQuery03 {

    private final String topic = "topic-tpch-query-03";

    public TPCHQuery03() {
        this(null, PARAMETER_OUTPUT_LOG, "127.0.0.1", false, false, -1);
    }

    public TPCHQuery03(String input, String output, String ipAddressSink, boolean disableOperatorChaining, boolean pinningPolicy, long maxCount) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

            if (disableOperatorChaining) {
                env.disableOperatorChaining();
            }

            DataStream<Order> orders = env
                    .addSource(new OrdersSource(input, maxCount)).name(OrdersSource.class.getSimpleName()).uid(OrdersSource.class.getSimpleName());

            // Filter market segment "AUTOMOBILE"
            // customers = customers.filter(new CustomerFilter());

            // Filter all Orders with o_orderdate < 12.03.1995
            DataStream<Order> ordersFiltered = orders
                    .filter(new OrderDateFilter("1995-03-12")).name(OrderDateFilter.class.getSimpleName()).uid(OrderDateFilter.class.getSimpleName());

            // Join customers with orders and package them into a ShippingPriorityItem
            DataStream<ShippingPriorityItem> customerWithOrders = ordersFiltered
                    .keyBy(new OrderKeySelector())
                    .process(new OrderKeyedByCustomerProcessFunction(pinningPolicy)).name(OrderKeyedByCustomerProcessFunction.class.getSimpleName()).uid(OrderKeyedByCustomerProcessFunction.class.getSimpleName());

            // Join the last join result with Lineitems
            DataStream<ShippingPriorityItem> result = customerWithOrders
                    .keyBy(new ShippingPriorityOrderKeySelector())
                    .process(new ShippingPriorityKeyedProcessFunction(pinningPolicy)).name(ShippingPriorityKeyedProcessFunction.class.getSimpleName()).uid(ShippingPriorityKeyedProcessFunction.class.getSimpleName());

            // Group by l_orderkey, o_orderdate and o_shippriority and compute revenue sum
            DataStream<ShippingPriorityItem> resultSum = result
                    .keyBy(new ShippingPriority3KeySelector())
                    .reduce(new SumShippingPriorityItem(pinningPolicy)).name(SumShippingPriorityItem.class.getSimpleName()).uid(SumShippingPriorityItem.class.getSimpleName());

            // emit result
            if (output.equalsIgnoreCase(PARAMETER_OUTPUT_MQTT)) {
                resultSum
                        .map(new ShippingPriorityItemMap(pinningPolicy)).name(ShippingPriorityItemMap.class.getSimpleName()).uid(ShippingPriorityItemMap.class.getSimpleName())
                        .addSink(new MqttStringPublisher(ipAddressSink, topic, pinningPolicy)).name(OPERATOR_SINK).uid(OPERATOR_SINK);
            } else if (output.equalsIgnoreCase(PARAMETER_OUTPUT_LOG)) {
                resultSum.print().name(OPERATOR_SINK).uid(OPERATOR_SINK);
            } else if (output.equalsIgnoreCase(PARAMETER_OUTPUT_FILE)) {
                StreamingFileSink<String> sink = StreamingFileSink
                        .forRowFormat(new Path(PATH_OUTPUT_FILE), new SimpleStringEncoder<String>("UTF-8"))
                        .withRollingPolicy(
                                DefaultRollingPolicy.builder().withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                        .withMaxPartSize(1024 * 1024 * 1024).build())
                        .build();

                resultSum
                        .map(new ShippingPriorityItemMap(pinningPolicy)).name(ShippingPriorityItemMap.class.getSimpleName()).uid(ShippingPriorityItemMap.class.getSimpleName())
                        .addSink(sink).name(OPERATOR_SINK).uid(OPERATOR_SINK);
            } else {
                System.out.println("discarding output");
            }

            System.out.println("Stream job: " + TPCHQuery03.class.getSimpleName());
            System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
            env.execute(TPCHQuery03.class.getSimpleName());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        new TPCHQuery03();
    }
}

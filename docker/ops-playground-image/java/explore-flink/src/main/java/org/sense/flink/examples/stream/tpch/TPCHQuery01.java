package org.sense.flink.examples.stream.tpch;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.sense.flink.examples.stream.tpch.pojo.LineItem;
import org.sense.flink.examples.stream.tpch.udf.*;
import org.sense.flink.mqtt.MqttStringPublisher;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.sense.flink.util.MetricLabels.OPERATOR_SINK;
import static org.sense.flink.util.SinkOutputs.*;

/**
 * Implementation of the TPC-H Benchmark query 01 also available at:
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
 * select
 *        l_returnflag,
 *        l_linestatus,
 *        sum(l_quantity) as sum_qty,
 *        sum(l_extendedprice) as sum_base_price,
 *        sum(l_extendedprice * (1-l_discount)) as sum_disc_price,
 *        sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge,
 *        avg(l_quantity) as avg_qty,
 *        avg(l_extendedprice) as avg_price,
 *        avg(l_discount) as avg_disc,
 *        count(*) as count_order
 *  from
 *        lineitem
 *  where
 *        l_shipdate <= mdy (12, 01, 1998 ) - 90 units day
 *  group by
 *        l_returnflag,
 *        l_linestatus
 *  order by
 *        l_returnflag,
 *        l_linestatus;
 * }
 * </pre>
 *
 * <p>
 * Compared to the original TPC-H query this version does not print c_phone and
 * c_comment, only filters by years greater than 1990 instead of a period of 3
 * months, and does not sort the result by revenue.
 *
 * @author Felipe Oliveira Gutierrez
 */
public class TPCHQuery01 {
    private final String topic = "topic-tpch-query-01";

    public TPCHQuery01() {
        this(null, PARAMETER_OUTPUT_LOG, "127.0.0.1", false, false, -1);
    }

    public TPCHQuery01(String input, String output, String ipAddressSink, boolean disableOperatorChaining, boolean pinningPolicy, long maxCount) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

            if (disableOperatorChaining) {
                env.disableOperatorChaining();
            }

            DataStream<LineItem> lineItems = env
                    .addSource(new LineItemSource(maxCount)).name(LineItemSource.class.getSimpleName()).uid(LineItemSource.class.getSimpleName());

            DataStream<Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> lineItemsMap = lineItems
                    .map(new LineItemToTuple11Map()).name(LineItemToTuple11Map.class.getSimpleName()).uid(LineItemToTuple11Map.class.getSimpleName());

            DataStream<Tuple11<String, String, Long, Double, Double, Double, Double, Long, Double, Double, Long>> sumAndAvgLineItems = lineItemsMap
                    .keyBy(new LineItemFlagAndStatusKeySelector())
                    .reduce(new SumAndAvgLineItemReducer()).name(SumAndAvgLineItemReducer.class.getSimpleName()).uid(SumAndAvgLineItemReducer.class.getSimpleName());

            DataStream<String> result = sumAndAvgLineItems
                    .map(new Tuple11ToLineItemResult()).name(Tuple11ToLineItemResult.class.getSimpleName()).uid(Tuple11ToLineItemResult.class.getSimpleName());

            if (output.equalsIgnoreCase(PARAMETER_OUTPUT_MQTT)) {
                result.addSink(new MqttStringPublisher(ipAddressSink, topic, pinningPolicy)).name(OPERATOR_SINK).uid(OPERATOR_SINK);
            } else if (output.equalsIgnoreCase(PARAMETER_OUTPUT_LOG)) {
                result.print().name(OPERATOR_SINK).uid(OPERATOR_SINK);
            } else if (output.equalsIgnoreCase(PARAMETER_OUTPUT_FILE)) {
                StreamingFileSink<String> sink = StreamingFileSink
                        .forRowFormat(new Path(PATH_OUTPUT_FILE), new SimpleStringEncoder<String>("UTF-8"))
                        .withRollingPolicy(
                                DefaultRollingPolicy.builder().withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                        .withMaxPartSize(1024 * 1024 * 1024).build())
                        .build();

                result.addSink(sink).name(OPERATOR_SINK).uid(OPERATOR_SINK);
            } else {
                System.out.println("discarding output");
            }

            System.out.println("Stream job: " + TPCHQuery01.class.getSimpleName());
            System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
            env.execute(TPCHQuery01.class.getSimpleName());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        new TPCHQuery01();
    }
}

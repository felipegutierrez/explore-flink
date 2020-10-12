package org.sense.flink.examples.stream.tpch;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.sense.flink.examples.stream.tpch.pojo.Order;
import org.sense.flink.examples.stream.tpch.udf.*;
import org.sense.flink.mqtt.MqttStringPublisher;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;
import static org.sense.flink.util.MetricLabels.OPERATOR_REDUCER;
import static org.sense.flink.util.MetricLabels.OPERATOR_SINK;
import static org.sense.flink.util.SinkOutputs.*;

/**
 * Implementation of the TPC-H Benchmark query 10 also available at:
 * <p>
 * https://github.com/apache/flink/blob/master/flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java/relational/TPCHQuery10.java
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
 * SELECT FIRST 20
 *      c_custkey,
 *      c_name,
 *      SUM(l_extendedprice * (1 - l_discount)) AS revenue,
 *      c_acctbal,
 *      n_name,
 *      c_address,
 *      c_phone,
 *      c_comment
 * FROM
 *      customer,
 *      orders,
 *      lineitem,
 *      nation
 * WHERE
 *      c_custkey = o_custkey
 *      AND l_orderkey = o_orderkey
 *      AND o_orderdate >= MDY  (10,1,1993)
 *      AND o_orderdate < MDY(10,1,1993) + 3 UNITS MONTH
 *      AND l_returnflag = 'R'
 *      AND c_nationkey = n_nationkey
 * GROUP BY
 *      c_custkey,
 *      c_name,
 *      c_acctbal,
 *      c_phone,
 *      n_name,
 *      c_address,
 *      c_comment
 * ORDER BY
 *      revenue DESC
 * }
 * </pre>
 *
 * @author Felipe Oliveira Gutierrez
 */
public class TPCHQuery10 {
    private final String topic = "topic-tpch-query-10";

    public TPCHQuery10() {
        this(null, null, null, PARAMETER_OUTPUT_LOG, "127.0.0.1", false, false, -1);
    }

    public TPCHQuery10(String input, String topic, String bootstrapServers, String output, String ipAddressSink, boolean disableOperatorChaining, boolean pinningPolicy, long maxCount) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

            // use filesystem based state management
            // env.setStateBackend(new FsStateBackend("file:///tmp/flink/checkpoints"));

            if (disableOperatorChaining) {
                env.disableOperatorChaining();
            }

            DataStream<Order> orders = null;
            if ("kafka".equalsIgnoreCase(input)) {
                Properties properties = new Properties();
                // properties.setProperty("zookeeper.connect", "10.97.159.124:2181");
                properties.setProperty("bootstrap.servers", bootstrapServers);
                // properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                // not to be shared with another job consuming the same topic
                properties.setProperty("group.id", "flink-group");
                // properties.setProperty("security.protocol", "SSL");
                // properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                // properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                // orders =
                FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer<>(topic, new MyJSONDeserializationSchema(), properties);
                // FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer<>(topic, new JSONKeyValueDeserializationSchema(true), properties);
                // FlinkKafkaConsumer011 kafkaConsumer = new FlinkKafkaConsumer011<>(topic, new JSONKeyValueDeserializationSchema(false), properties);
                DataStream<ObjectNode> s = env.addSource(kafkaConsumer);
                s.print();
            } else {
                orders = env
                        .addSource(new OrdersSource(input, maxCount)).name(OrdersSource.class.getSimpleName()).uid(OrdersSource.class.getSimpleName());
            }

            /*
            // orders filtered by year: (orderkey, custkey) = order.f2.substring(0, 4)) > 1990
            DataStream<Order> ordersFiltered = orders
                    .filter(new OrderYearFilter(1990)).name(OrderYearFilter.class.getSimpleName()).uid(OrderYearFilter.class.getSimpleName());

            // join orders with lineitems: (custkey, revenue)
            DataStream<Tuple2<Integer, Double>> revenueByCustomer = ordersFiltered
                    .keyBy(new OrderKeySelector())
                    .process(new OrderKeyedByProcessFunction(pinningPolicy)).name(OrderKeyedByProcessFunction.class.getSimpleName()).uid(OrderKeyedByProcessFunction.class.getSimpleName());

            // sum the revenue by customers
            DataStream<Tuple2<Integer, Double>> revenueByCustomerSum = revenueByCustomer
                    .keyBy(0).sum(1).name(OPERATOR_REDUCER).uid(OPERATOR_REDUCER);

            // join customer with nation (custkey, name, address, nationname, acctbal)
            // join customer (with nation) with revenue (custkey, name, address, nationname, acctbal, revenue)
            DataStream<Tuple6<Integer, String, String, String, Double, Double>> result = revenueByCustomerSum
                    .keyBy(0)
                    .process(new JoinCustomerWithRevenueKeyedProcessFunction(pinningPolicy)).name(JoinCustomerWithRevenueKeyedProcessFunction.class.getSimpleName()).uid(JoinCustomerWithRevenueKeyedProcessFunction.class.getSimpleName());

            // emit result
            if (output.equalsIgnoreCase(PARAMETER_OUTPUT_MQTT)) {
                result
                        .map(new Tuple6ToStringMap(pinningPolicy)).name(Tuple6ToStringMap.class.getSimpleName()).uid(Tuple6ToStringMap.class.getSimpleName())
                        .addSink(new MqttStringPublisher(ipAddressSink, topic, pinningPolicy)).name(OPERATOR_SINK).uid(OPERATOR_SINK);
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

                result
                        .map(new Tuple6ToStringMap(pinningPolicy)).name(Tuple6ToStringMap.class.getSimpleName()).uid(Tuple6ToStringMap.class.getSimpleName())
                        .addSink(sink).name(OPERATOR_SINK).uid(OPERATOR_SINK);
            } else {
                System.out.println("discarding output");
            }*/

            System.out.println("Stream job: " + TPCHQuery10.class.getSimpleName());
            System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
            env.execute(TPCHQuery10.class.getSimpleName());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class MyJSONDeserializationSchema implements DeserializationSchema<ObjectNode> {

        private static final long serialVersionUID = -1L;

        private ObjectMapper mapper;

        @Override
        public ObjectNode deserialize(byte[] message) throws IOException {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            ObjectNode node = mapper.createObjectNode();
            if (message != null) {
                node.set("value", mapper.readValue(message, JsonNode.class));
            }
            return node;
        }

        @Override
        public boolean isEndOfStream(ObjectNode o) {
            return false;
        }

        @Override
        public TypeInformation getProducedType() {
            return getForClass(ObjectNode.class);
        }
    }

    public static void main(String[] args) throws Exception {
        String topic = "my-topic";
        String bootstrapServers = "10.111.85.76:9092";
        new TPCHQuery10("kafka", topic, bootstrapServers, "log", "127.0.0.1", false, false, -1);
    }
}

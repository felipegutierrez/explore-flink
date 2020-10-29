package org.sense.flink;

import org.sense.flink.examples.batch.MatrixMultiplication;
import org.sense.flink.examples.stream.clickcount.ClickEventCount;
import org.sense.flink.examples.stream.clickcount.ClickEventGenerator;
import org.sense.flink.examples.stream.edgent.*;
import org.sense.flink.examples.stream.kafka.KafkaConsumerQuery;
import org.sense.flink.examples.stream.tpch.TCPHOrderProducer;
import org.sense.flink.examples.stream.tpch.TPCHQuery01;
import org.sense.flink.examples.stream.tpch.TPCHQuery03;
import org.sense.flink.examples.stream.tpch.TPCHQuery10;
import org.sense.flink.examples.stream.twitter.TwitterExample;
import org.sense.flink.examples.stream.valencia.*;
import org.sense.flink.examples.table.TaxiRideCountTable;
import org.sense.flink.util.SinkOutputs;
import org.sense.flink.util.ValenciaItemType;

/**
 * <pre>
 * parameters to run this application:
 * -app 30 -source 127.0.0.1 -sink 127.0.0.1 -offlineData true -frequencyPull 10 -frequencyWindow 30 -syntheticData true
 * </pre>
 *
 * @author Felipe Oliveira Gutierrez
 */
public class App {
    private static final String PARAMETER_APP = "-app";
    private static final String PARAMETER_SOURCE = "-source";
    private static final String PARAMETER_SINK = "-sink";
    private static final String PARAMETER_OFFLINE_DATA = "-offlineData";
    private static final String PARAMETER_FREQUENCY_PULL = "-frequencyPull";
    private static final String PARAMETER_FREQUENCY_WINDOW = "-frequencyWindow";
    private static final String PARAMETER_SYNTHETIC_DATA = "-syntheticData";
    private static final String PARAMETER_ENABLE_OPTIMIZATION = "-optimization";
    private static final String PARAMETER_LOOKUP_TABLE = "-lookup";
    private static final String PARAMETER_PARALLELISM = "-parallelism";
    private static final String PARAMETER_DISABLE_OPERATOR_CHAINING = "-disableOperatorChaining";
    private static final String PARAMETER_OUTPUT = "-output";
    private static final String PARAMETER_INPUT = "-input";
    private static final String PARAMETER_BOOTSTRAP_SERVERS = "-bootstrap.servers";
    private static final String PARAMETER_TOPIC = "-topic";
    private static final String PARAMETER_DURATION = "-duration";
    private static final String PARAMETER_MAX_COUNT = "-maxCount";
    private static final String PARAMETER_PINNING_POLICY = "-pinningPolicy";
    private static final String PARAMETER_SKEWED_DATA = "-skew";

    public static void main(String[] args) throws Exception {

        String ipAddressSource = "127.0.0.1";
        String ipAddressSink = "127.0.0.1";
        String output = SinkOutputs.PARAMETER_OUTPUT_FILE;
        String input = null;
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "topic_default";
        int app = 0;
        int frequencyPull = 10;
        int frequencyWindow = 30;
        int parallelism = 0;
        long duration = Long.MAX_VALUE;
        long maxCount = -1;
        boolean offlineData = false;
        boolean syntheticData = false;
        boolean optimization = true;
        boolean lookup = true;
        boolean disableOperatorChaining = false;
        boolean pinningPolicy = false;
        boolean skewedData = false;

        if (args != null && args.length > 0) {
            int size = args.length;
            for (int i = 0; i < size; i++) {
                if (PARAMETER_APP.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    app = Integer.parseInt(args[i]);
                } else if (PARAMETER_SOURCE.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    ipAddressSource = String.valueOf(args[i]);
                    if (!validIP(ipAddressSource)) {
                        System.err.println("IP address invalid[" + ipAddressSource + "]");
                    }
                } else if (PARAMETER_SINK.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    ipAddressSink = String.valueOf(args[i]);
                    if (!validIP(ipAddressSink)) {
                        System.err.println("IP address invalid[" + ipAddressSink + "]");
                    }
                } else if (PARAMETER_OFFLINE_DATA.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    offlineData = Boolean.valueOf(args[i]);
                } else if (PARAMETER_FREQUENCY_PULL.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    frequencyPull = Integer.parseInt(args[i]);
                } else if (PARAMETER_ENABLE_OPTIMIZATION.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    optimization = Boolean.valueOf(args[i]);
                } else if (PARAMETER_DISABLE_OPERATOR_CHAINING.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    disableOperatorChaining = Boolean.valueOf(args[i]);
                } else if (PARAMETER_PINNING_POLICY.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    pinningPolicy = Boolean.valueOf(args[i]);
                } else if (PARAMETER_SKEWED_DATA.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    skewedData = Boolean.valueOf(args[i]);
                } else if (PARAMETER_FREQUENCY_WINDOW.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    frequencyWindow = Integer.parseInt(args[i]);
                } else if (PARAMETER_SYNTHETIC_DATA.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    syntheticData = Boolean.valueOf(args[i]);
                } else if (PARAMETER_LOOKUP_TABLE.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    lookup = Boolean.valueOf(args[i]);
                } else if (PARAMETER_PARALLELISM.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    parallelism = Integer.parseInt(args[i]);
                } else if (PARAMETER_DURATION.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    duration = Long.parseLong(args[i]);
                } else if (PARAMETER_MAX_COUNT.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    maxCount = Long.parseLong(args[i]);
                } else if (PARAMETER_INPUT.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    input = String.valueOf(args[i]);
                } else if (PARAMETER_BOOTSTRAP_SERVERS.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    bootstrapServers = String.valueOf(args[i]);
                } else if (PARAMETER_TOPIC.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    topic = String.valueOf(args[i]);
                } else if (PARAMETER_OUTPUT.equals(String.valueOf(args[i])) && i + 1 < size) {
                    i++;
                    if (SinkOutputs.PARAMETER_OUTPUT_FILE.equals(String.valueOf(args[i]))) {
                        output = SinkOutputs.PARAMETER_OUTPUT_FILE;
                    } else if (SinkOutputs.PARAMETER_OUTPUT_MQTT.equals(String.valueOf(args[i]))) {
                        output = SinkOutputs.PARAMETER_OUTPUT_MQTT;
                    } else if (SinkOutputs.PARAMETER_OUTPUT_LOG.equals(String.valueOf(args[i]))) {
                        output = SinkOutputs.PARAMETER_OUTPUT_LOG;
                    }
                }
            }
        } else {
            listApplications();
        }
        System.out.println();
        System.out.println("Parameters chosen >>");
        System.out.println("Application selected    : " + app);
        System.out.println("duration[minutes]       : " + duration);
        System.out.println("maxCount to read source : " + maxCount);
        System.out.println("ipAddressSource         : " + ipAddressSource);
        System.out.println("ipAddressSink           : " + ipAddressSink);
        System.out.println("offlinedata             : " + offlineData);
        System.out.println("syntheticData           : " + syntheticData);
        System.out.println("skewedData              : " + skewedData);
        System.out.println("parallelism             : " + parallelism);
        System.out.println("disableOperatorChaining : " + disableOperatorChaining);
        System.out.println("frequencyPull[seconds]  : " + frequencyPull);
        System.out.println("frequencyWindow[seconds]: " + frequencyWindow);
        System.out.println("optimization            : " + optimization);
        System.out.println("lookup                  : " + lookup);
        System.out.println("pinning policy          : " + pinningPolicy);
        System.out.println("output                  : " + output);
        System.out.println("input                   : " + input);
        System.out.println();

        try {
            switch (app) {
                case 0:
                    System.out.println("Parameters missing! Please launch the application following the example below.");
                    System.out.println(
                            "./bin/flink run -c org.sense.flink.App explore-flink.jar -app 30 -source 130.239.48.136 -sink 130.239.48.136 -offlineData true -frequencyPull 10 -frequencyWindow 30 -syntheticData true");
                    System.out.println("bis später");
                    break;
                case 1:
                    System.out.println("App 1 selected");
                    new WordCountFilterQEP();
                    app = 0;
                    break;
                case 2:
                    System.out.println("App 2 selected");
                    new WordCountMqttFilterQEP();
                    app = 0;
                    break;
                case 3:
                    System.out.println("App 3 selected");
                    new MatrixMultiplication();
                    app = 0;
                    break;
                case 4:
                    System.out.println("App 4 selected");
                    new SensorsReadingMqttJoinQEP();
                    app = 0;
                    break;
                case 5:
                    System.out.println("App 5 selected");
                    new SensorsReadingMqttEdgentQEP();
                    app = 0;
                    break;
                case 6:
                    System.out.println("App 6 selected");
                    System.out.println("use 'mosquitto_pub -h 127.0.0.1 -t topic-parameter -m \"0.0,1000.0\"' ");
                    System.out.println("on the terminal to change the parameters at runtime.");
                    new SensorsDynamicFilterMqttEdgentQEP();
                    app = 0;
                    break;
                case 7:
                    System.out.println("App 7 selected");
                    System.out.println("The adaptive filter was pushed down to the data source");
                    new AdaptiveFilterRangeMqttEdgent();
                    app = 0;
                    break;
                case 8:
                    System.out.println("App 8 selected");
                    System.out.println("Temperature average example");
                    new TemperatureAverageExample();
                    app = 0;
                    break;
                case 9:
                    // @formatter:off
                    System.out.println("App 9 selected");
                    System.out.println("Use [./bin/flink run examples/explore-flink.jar 9 -c] to run this program on the Flink standalone-cluster");
                    System.out.println("Consuming values from 3 MQTT topics");
                    // @formatter:on
                    new SensorsMultipleReadingMqttEdgentQEP();
                    app = 0;
                    break;
                case 10:
                    // @formatter:off
                    System.out.println("App 10 selected");
                    System.out.println("Use [./bin/flink run examples/explore-flink.jar 10 -c] to run this program on the Flink standalone-cluster");
                    System.out.println("Consuming values from 3 MQTT topics");
                    // @formatter:on
                    new SensorsMultipleReadingMqttEdgentQEP2();
                    app = 0;
                    break;
                case 11:
                    // @formatter:off
                    System.out.println("App 11 selected (ValueState)");
                    System.out.println("Use [./bin/flink run examples/explore-flink.jar 11 -c] to run this program on the Flink standalone-cluster");
                    System.out.println("Consuming values from 2 MQTT topics");
                    // @formatter:on
                    new MultiSensorMultiStationsReadingMqtt();
                    app = 0;
                    break;
                case 12:
                    // @formatter:off
                    System.out.println("App 12 selected (window)");
                    System.out.println("Use [./bin/flink run examples/explore-flink.jar 12 -c] to run this program on the Flink standalone-cluster");
                    System.out.println("Consuming values from 2 MQTT topics");
                    // @formatter:on
                    new MultiSensorMultiStationsReadingMqtt2();
                    app = 0;
                    break;
                case 13:
                    // @formatter:off
                    System.out.println("App 13 selected (join within a window)");
                    System.out.println("Use [./bin/flink run examples/explore-flink.jar 13 -c] to run this program on the Flink standalone-cluster");
                    System.out.println("Consuming values from 2 MQTT topics");
                    // @formatter:on
                    new MultiSensorMultiStationsJoinMqtt();
                    app = 0;
                    break;
                case 14:
                    new MqttSensorDataSkewedJoinDAG(ipAddressSource, ipAddressSink);
                    app = 0;
                    break;
                case 15:
                    new MqttSensorDataSkewedPartitionByKeyDAG(ipAddressSource, ipAddressSink);
                    app = 0;
                    break;
                case 16:
                    new MqttSensorDataSkewedRescaleByKeyDAG(ipAddressSource, ipAddressSink);
                    app = 0;
                    break;
                case 17:
                    new MqttSensorDataSkewedPartitionByKeySkewedDAG(ipAddressSource, ipAddressSink);
                    app = 0;
                    break;
                case 18:
                    new MqttSensorDataSkewedCombinerByKeySkewedDAG(ipAddressSource, ipAddressSink);
                    app = 0;
                    break;
                case 19:
                    new MqttSensorDataCombinerByKeySkewedDAG(ipAddressSource, ipAddressSink);
                    app = 0;
                    break;
                case 20:
                    System.out.println("application 20");
                    new TwitterExample(args);
                    app = 0;
                    break;
                case 21:
                    System.out.println("Estimate cardinality with HyperLogLog");
                    new MqttSensorDataHLLKeyedProcessWindow(ipAddressSource, ipAddressSink);
                    app = 0;
                    break;
                case 24:
                    new TaxiRideCountTable(ipAddressSink);
                    app = 0;
                    break;
                case 25:
                    new ValenciaDataSkewedJoinExample(ipAddressSource, ipAddressSink);
                    app = 0;
                    break;
                case 26:
                    new ValenciaDataSkewedCombinerExample(ipAddressSource, ipAddressSink, offlineData, frequencyPull,
                            frequencyWindow, syntheticData, optimization, duration);
                    app = 0;
                    break;
                case 27:
                    new ValenciaDataSkewedRepartitionJoinExample(ipAddressSource, ipAddressSink);
                    app = 0;
                    break;
                case 28:
                    new ValenciaDataSkewedBroadcastJoinExample(ipAddressSource, ipAddressSink, duration);
                    app = 0;
                    break;
                case 29:
                    new ValenciaBloomFilterLookupJoinExample(ipAddressSource, ipAddressSink, offlineData, frequencyPull,
                            frequencyWindow, parallelism, disableOperatorChaining, syntheticData, optimization, lookup,
                            duration);
                    app = 0;
                    break;
                case 30:
                    new ValenciaDataCpuIntensiveJoinExample(ipAddressSource, ipAddressSink, offlineData, frequencyPull,
                            frequencyWindow, parallelism, disableOperatorChaining, output);
                    app = 0;
                    break;
                case 31:
                    new ValenciaBloomFilterSemiJoinExample(ipAddressSource, ipAddressSink, offlineData, frequencyPull,
                            frequencyWindow, parallelism, disableOperatorChaining, syntheticData, optimization, lookup,
                            duration);
                    app = 0;
                    break;
                case 32:
                    ValenciaDataProducer producerTrafficJam = new ValenciaDataProducer(ValenciaItemType.TRAFFIC_JAM,
                            offlineData, maxCount, skewedData);
                    producerTrafficJam.connect();
                    producerTrafficJam.start();
                    producerTrafficJam.publish();
                    producerTrafficJam.disconnect();
                    app = 0;
                    break;
                case 33:
                    ValenciaDataProducer producerPollution = new ValenciaDataProducer(ValenciaItemType.AIR_POLLUTION,
                            offlineData, maxCount, skewedData);
                    producerPollution.connect();
                    producerPollution.start();
                    producerPollution.publish();
                    producerPollution.disconnect();
                    app = 0;
                    break;
                case 34:
                    new ValenciaDataMqttCpuIntensiveJoinExample(ipAddressSource, ipAddressSink, frequencyWindow,
                            parallelism, disableOperatorChaining, output, pinningPolicy);
                    app = 0;
                    break;
                case 35:
                    new KafkaConsumerQuery();
                    app = 0;
                    break;
                case 36:
                    new TPCHQuery03(input, output, ipAddressSink, disableOperatorChaining, pinningPolicy, maxCount);
                    app = 0;
                    break;
                case 37:
                    new TPCHQuery10(input, topic, bootstrapServers, output, ipAddressSink, disableOperatorChaining, pinningPolicy, maxCount);
                    app = 0;
                    break;
                case 38:
                    new TPCHQuery01(input, output, ipAddressSink, disableOperatorChaining, pinningPolicy, maxCount);
                    app = 0;
                    break;
                case 39:
                    new ClickEventGenerator();
                    app = 0;
                    break;
                case 40:
                    new ClickEventCount();
                    app = 0;
                    break;
                case 41:
                    new TCPHOrderProducer(bootstrapServers, topic);
                    app = 0;
                    break;
                default:
                    args = null;
                    System.out.println("No application selected [" + app + "] ");
                    break;
            }
        } catch (Exception ce) {
            System.err.println(ce.getMessage());
            ce.printStackTrace();
        }
    }

    private static void listApplications() {
        System.out.println("Applications available");
        System.out.println("0  - exit");
        System.out.println("1  - World count (Sokect stream) with Filter and QEP");
        System.out.println("2  - World count (MQTT stream) with Filter and QEP");
        System.out.println("3  - Matrix multiplication using batch and QEP");
        System.out.println("4  - Two fake sensors (MQTT stream) and QEP");
        System.out.println("5  - Fake sensor from Apache Edgent (MQTT stream) and QEP");
        System.out.println("6  - Dynamic filter over fake data source and QEP");
        System.out.println("7  - Adaptive filter pushed down to Apache Edgent");
        System.out.println("8  - Temperature average example");
        System.out.println("9  - Consume MQTT from multiple temperature sensors");
        System.out.println("10 - Consume MQTT from multiple temperature sensors");
        System.out.println("11 - Consume MQTT from multiple sensors at train stations with ValueState");
        System.out.println("12 - Consume MQTT from multiple sensors at train stations with window");
        System.out.println("13 - Consume MQTT from multiple sensors at train stations and join within a window");
        System.out.println("14 - Consume MQTT from multiple sensors at train stations and join within a window");
        System.out.println("15 - Partition by Key and Reducing over a window");
        System.out.println("16 - Custom Partition by Key and Reducing over a window");
        System.out.println("17 - Random Partition by Key and Reducing over a window");
        System.out.println("18 - Combiner on the map phase just before shuffling and Reducing over a window");
        System.out.println("19 - Combiner on the map phase just before shuffling and Reducing over a window with RichFunction");
        System.out.println("20 - TwitterExample");
        System.out.println("21 - Estimate cardinality with HyperLogLog");
        System.out.println("22 - Estimate cardinality with HyperLogLogPlus");
        System.out.println("23 - Estimate cardinality with Bloom Filter");
        System.out.println("24 - Aggregation query for Taxi Ride Count using Flink Table API");
        System.out.println("25 - Reading values from Valencia Open-data Web Portal and processing a JOIN using Flink Data Stream");
        System.out.println("26 - Reading values from Valencia Open-data Web Portal and processing a COMBINER using Flink Data Stream");
        System.out.println("27 - Reading values from Valencia Open-data Web Portal and computing the Standard Repartition JOIN using Flink Data Stream");
        System.out.println("28 - Reading values from Valencia Open-data Web Portal and computing the Broadcast JOIN using Flink Data Stream");
        System.out.println("29 - Reading values from Valencia Open-data Web Portal and computing the Lookup-JOIN with Bloom Filter using Flink Data Stream");
        System.out.println("30 - Reading values from Valencia Open-data Web Portal and CPU intensive computation using Flink Data Stream");
        System.out.println("31 - Reading values from Valencia Open-data Web Portal and computing the semi-JOIN with Bloom Filter using Flink Data Stream");
        System.out.println("32 - Mqtt data source of traffic-jam from Valencia Open-data Web Portal");
        System.out.println("33 - Mqtt data source of pollution from Valencia Open-data Web Portal");
        System.out.println("34 - CPU intensive computation which consumes data from Valencia Open-data Web Portal");
        System.out.println("35 - Kafka source");
        System.out.println("36 - TPC-H Benchmark query 03");
        System.out.println("37 - TPC-H Benchmark query 10");
        System.out.println("38 - TPC-H Benchmark query 01");
        System.out.println("39 - ClickEventGenerator using Kafka");
        System.out.println("40 - ClickEventCount consuming from Kafka");
        System.out.println("41 - TCP-H Order Producer from Kafka");
        System.out.println();
    }

    public static boolean validIP(String ip) {
        try {
            if (ip == null || ip.isEmpty()) {
                return false;
            }

            String[] parts = ip.split("\\.");
            if (parts.length != 4) {
                return false;
            }

            for (String s : parts) {
                int i = Integer.parseInt(s);
                if ((i < 0) || (i > 255)) {
                    return false;
                }
            }
            return !ip.endsWith(".");
        } catch (NumberFormatException nfe) {
            return false;
        }
    }
}

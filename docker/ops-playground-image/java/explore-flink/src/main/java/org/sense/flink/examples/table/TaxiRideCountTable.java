package org.sense.flink.examples.table;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.sense.flink.examples.stream.udf.MqttDataSink;
import org.sense.flink.examples.stream.udf.TaxiRideCountMap;
import org.sense.flink.examples.table.udf.TaxiRideSource;
import org.sense.flink.examples.table.util.TaxiRide;
import org.sense.flink.examples.table.util.TaxiRideCommons;

import static org.apache.flink.table.api.Expressions.$;
import static org.sense.flink.util.MetricLabels.*;

/**
 * change the flink-table-* in the pom.xml from <scope>provided</scope> to <scope>compile</scope>
 */
public class TaxiRideCountTable {

    public TaxiRideCountTable() {
        this(TaxiRideCommons.pathToRideData, "127.0.0.1", 1883, true, 4, true, "1 s", "1000", true);
    }

    public TaxiRideCountTable(String sinkHost) {
        this(TaxiRideCommons.pathToRideData, sinkHost, 1883, true, 4, true, "1 s", "1000", true);
    }

    public TaxiRideCountTable(String input, String sinkHost, boolean disableOperatorChaining, int parallelism, boolean mini_batch_enabled, String mini_batch_allow_latency, String mini_batch_size, boolean twoPhaseAgg) {
        this(input, sinkHost, 1883, disableOperatorChaining, parallelism, mini_batch_enabled, mini_batch_allow_latency, mini_batch_size, twoPhaseAgg);
    }

    public TaxiRideCountTable(String input, String sinkHost, int sinkPort, boolean disableOperatorChaining, int parallelism,
                              boolean mini_batch_enabled, String mini_batch_allow_latency, String mini_batch_size, boolean twoPhaseAgg) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            // access flink configuration
            Configuration configuration = tableEnv.getConfig().getConfiguration();
            // set low-level key-value options
            configuration.setInteger("table.exec.resource.default-parallelism", parallelism);
            // local-global aggregation depends on mini-batch is enabled
            configuration.setString("table.exec.mini-batch.enabled", Boolean.toString(mini_batch_enabled));
            configuration.setString("table.exec.mini-batch.allow-latency", mini_batch_allow_latency);
            configuration.setString("table.exec.mini-batch.size", mini_batch_size);
            // enable two-phase, i.e. local-global aggregation
            if (twoPhaseAgg) {
                configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
            }

            if (disableOperatorChaining) {
                env.disableOperatorChaining();
            }

            DataStream<TaxiRide> ridesStream = env.addSource(new TaxiRideSource(input)).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE);

            // "rideId, isStart, startTime, endTime, startLon, startLat, endLon, endLat, passengerCnt, taxiId, driverId"
            Table ridesTableStream = tableEnv.fromDataStream(ridesStream);

            Table resultTableStream = ridesTableStream
                    .groupBy($("taxiId"))
                    .select($("taxiId"), $("passengerCnt").count().as("passengerCnt"));

            // DataStream<TaxiRide> result = tableEnv.toAppendStream(resultTableStream, TaxiRide.class);
            TypeInformation<Tuple2<Long, Long>> typeInfo = TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
            });
            tableEnv
                    .toRetractStream(resultTableStream, typeInfo)
                    .map(new TaxiRideCountMap()).name(OPERATOR_MAP_OUTPUT).uid(OPERATOR_MAP_OUTPUT)
                    .addSink(new MqttDataSink(TOPIC_DATA_SINK, sinkHost, sinkPort)).name(OPERATOR_SINK).uid(OPERATOR_SINK);

            System.out.println(env.getExecutionPlan());
            env.execute(TaxiRideCountTable.class.getSimpleName());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        TaxiRideCountTable taxiRideCountTable = new TaxiRideCountTable();
    }
}

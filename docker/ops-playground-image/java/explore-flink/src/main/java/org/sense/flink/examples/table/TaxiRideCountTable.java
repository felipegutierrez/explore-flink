package org.sense.flink.examples.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.sense.flink.examples.table.udf.TaxiRideSource;
import org.sense.flink.examples.table.util.TaxiRide;
import org.sense.flink.examples.table.util.TaxiRideCommons;

import static org.sense.flink.examples.table.util.TaxiRideCommons.*;
import static org.sense.flink.util.MetricLabels.OPERATOR_SOURCE;

public class TaxiRideCountTable {

    final String input = TaxiRideCommons.pathToRideData;
    private boolean disableOperatorChaining;
    private int slotSplit;

    public TaxiRideCountTable() {
        this(true, 0);
    }

    public TaxiRideCountTable(boolean disableOperatorChaining, int slotSplit) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            if (disableOperatorChaining) {
                env.disableOperatorChaining();
            }
            String slotGroup01 = SLOT_GROUP_DEFAULT;
            String slotGroup02 = SLOT_GROUP_DEFAULT;
            if (slotSplit == 0) {
                slotGroup01 = SLOT_GROUP_DEFAULT;
                slotGroup02 = SLOT_GROUP_DEFAULT;
            } else if (slotSplit == 1) {
                slotGroup01 = SLOT_GROUP_01_01;
                slotGroup02 = SLOT_GROUP_DEFAULT;
            } else if (slotSplit == 2) {
                slotGroup01 = SLOT_GROUP_01_01;
                slotGroup02 = SLOT_GROUP_01_02;
            }

            DataStream<TaxiRide> ridesStream = env.addSource(new TaxiRideSource(input)).name(OPERATOR_SOURCE).uid(OPERATOR_SOURCE).slotSharingGroup(slotGroup01);

            Table ridesTableStream = tableEnv.fromDataStream(ridesStream, "rideId, isStart, startTime, endTime, startLon, startLat, endLon, endLat, passengerCnt, taxiId, driverId");
            Table resultTableStream = tableEnv.sqlQuery("SELECT * from " + ridesTableStream);
            tableEnv.toAppendStream(resultTableStream, ridesStream.getType()).print();

            System.out.println(env.getExecutionPlan());
            env.execute(TaxiRideCountTable.class.getSimpleName());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new TaxiRideCountTable();
    }
}

package org.sense.flink.examples.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TaxiRideCountTable {

    private boolean disableOperatorChaining;

    public TaxiRideCountTable() {
        this(true);
    }

    public TaxiRideCountTable(boolean disableOperatorChaining) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            if (disableOperatorChaining) {
                env.disableOperatorChaining();
            }

            DataStream<Tuple2<Long, String>> ds = env.fromElements(
                    Tuple2.of(1L, "a"),
                    Tuple2.of(2L, "b"),
                    Tuple2.of(3L, "c")
            );
            Table table = tableEnv.fromDataStream(ds, "id, name");
            Table result = tableEnv.sqlQuery("SELECT * from " + table);
            tableEnv.toAppendStream(result, ds.getType()).print();

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

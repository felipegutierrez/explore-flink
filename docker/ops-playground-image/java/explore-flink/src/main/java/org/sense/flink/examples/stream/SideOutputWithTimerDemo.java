package org.sense.flink.examples.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.sense.flink.examples.stream.udf.stackoverflow.SideOutputWithTimer;

public class SideOutputWithTimerDemo {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final OutputTag<String> outputTag = new OutputTag<String>("side-output") {
        };

        // DataStream<Tuple2<String, String>> stream = env.addSource();

        // DataStream<Tuple2<String, Long>> result =
                /*stream
                .keyBy(value -> value.f0)
                .process(new SideOutputWithTimer())
                ;*/

    }
}

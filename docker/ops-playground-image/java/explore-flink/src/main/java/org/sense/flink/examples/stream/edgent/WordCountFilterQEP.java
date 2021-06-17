package org.sense.flink.examples.stream.edgent;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.sense.flink.source.WordsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class WordCountFilterQEP {

    public WordCountFilterQEP() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .addSource(new WordsSource(1))
                .flatMap(new SplitterFlatMap())
                .process(new ExceptionSimulatorProcess(System.currentTimeMillis() - 10_000L, 2_000L))
                .keyBy(new WordKeySelector()) // select the first value as a key
                .reduce(new SumReducer()) // reduce to sum all values with same key
                // .filter(word -> word.f1 >= 2) // use simple filter
                ;

        String executionPlan = env.getExecutionPlan();
        System.out.println("ExecutionPlan ........................ ");
        System.out.println(executionPlan);
        System.out.println("........................ ");

        dataStream.print();

        env.execute(WordCountFilterQEP.class.getSimpleName());
    }

    public static void main(String[] args) throws Exception {
        WordCountFilterQEP app = new WordCountFilterQEP();
    }

    public static class SplitterFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

    public static class WordKeySelector implements KeySelector<Tuple2<String, Integer>, String> {

        @Override
        public String getKey(Tuple2<String, Integer> value) throws Exception {
            return value.f0;
        }
    }

    public static class SumReducer implements ReduceFunction<Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
            return Tuple2.of(value1.f0, value1.f1 + value2.f1);
        }
    }
}

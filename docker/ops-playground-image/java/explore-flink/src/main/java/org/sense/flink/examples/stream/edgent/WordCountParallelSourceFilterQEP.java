package org.sense.flink.examples.stream.edgent;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.sense.flink.source.WordsParallelSource;

public class WordCountParallelSourceFilterQEP {

    public WordCountParallelSourceFilterQEP() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .addSource(new WordsParallelSource(1))
                .flatMap(new SplitterFlatMap())
                .keyBy(new WordKeySelector()) // select the first value as a key
                .reduce(new SumReducer()) // reduce to sum all values with same key
                // .filter(word -> word.f1 >= 2) // use simple filter
                ;

        String executionPlan = env.getExecutionPlan();
        System.out.println("ExecutionPlan ........................ ");
        System.out.println(executionPlan);
        System.out.println("........................ ");

        dataStream.print();

        env.execute(WordCountParallelSourceFilterQEP.class.getSimpleName());
    }

    public static void main(String[] args) throws Exception {
        WordCountParallelSourceFilterQEP app = new WordCountParallelSourceFilterQEP();
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

package org.sense.flink.examples.stream.edgent;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.sense.flink.source.WordsSource;

/**
 * On the terminal execute "nc -lk 9000", run this class and type words back on
 * the terminal
 *
 * @author Felipe Oliveira Gutierrez
 */
public class WordCountFilterQEP {

    public WordCountFilterQEP() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .addSource(new WordsSource())
                .flatMap(new SplitterFlatMap())
                .keyBy(0) // select the first value as a key
                .sum(1) // reduce to sum all values with same key
                // .filter(word -> word.f1 >= 2) // use simple filter
                ;

        String executionPlan = env.getExecutionPlan();
        System.out.println("ExecutionPlan ........................ ");
        System.out.println(executionPlan);
        System.out.println("........................ ");

        dataStream.print();

        env.execute("WordCountSocketFilterQEP");
    }

    public static void main(String[] args) throws Exception {
        WordCountFilterQEP app = new WordCountFilterQEP();
    }

    public static class SplitterFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {

        private static final long serialVersionUID = -6155646115486510443L;

        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}

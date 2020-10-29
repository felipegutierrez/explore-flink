package org.sense.flink.examples.stream.edgent;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.sense.flink.source.WordsSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class WordCountFilterQEPTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster;
    private final int minAvailableProcessors = 4;
    private final boolean runInParallel;

    public WordCountFilterQEPTest() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        this.runInParallel = availableProcessors >= minAvailableProcessors;
        if (this.runInParallel) {
            flinkCluster = new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(minAvailableProcessors)
                            .setNumberTaskManagers(1)
                            .build());
        }
    }

    @Test
    public void testSplitFlatMap() throws Exception {
        // instantiate your function
        WordCountFilterQEP.SplitterFlatMap splitterFlatMap = new WordCountFilterQEP.SplitterFlatMap();

        String sentence = "this is a sentence this sentence must be split because this is a split sentence flatmap";
        List<Tuple2<String, Integer>> out = new ArrayList<Tuple2<String, Integer>>();
        ListCollector<Tuple2<String, Integer>> listCollector = new ListCollector<Tuple2<String, Integer>>(out);

        // call the methods that you have implemented
        splitterFlatMap.flatMap(sentence, listCollector);

        List<Tuple2<String, Integer>> outExpected = new ArrayList<Tuple2<String, Integer>>();
        outExpected.add(Tuple2.of("this", 1));
        outExpected.add(Tuple2.of("is", 1));
        outExpected.add(Tuple2.of("a", 1));
        outExpected.add(Tuple2.of("sentence", 1));
        outExpected.add(Tuple2.of("this", 1));
        outExpected.add(Tuple2.of("sentence", 1));
        outExpected.add(Tuple2.of("must", 1));
        outExpected.add(Tuple2.of("be", 1));
        outExpected.add(Tuple2.of("split", 1));
        outExpected.add(Tuple2.of("because", 1));
        outExpected.add(Tuple2.of("this", 1));
        outExpected.add(Tuple2.of("is", 1));
        outExpected.add(Tuple2.of("a", 1));
        outExpected.add(Tuple2.of("split", 1));
        outExpected.add(Tuple2.of("sentence", 1));
        outExpected.add(Tuple2.of("flatmap", 1));

        // verify collector was called with the right output
        Assert.assertEquals(outExpected, out);
    }

    @Test
    public void countWordsUsingWordsSource() throws Exception {
        if (this.runInParallel) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // configure your test environment
            env.setParallelism(4);

            // values are collected in a static variable
            CollectSink.values.clear();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource(new WordsSource("/shorttext.txt", 1))
                    .flatMap(new WordCountFilterQEP.SplitterFlatMap())
                    .keyBy(0)
                    .sum(1);
            dataStream.addSink(new CollectSink());

            // execute
            env.execute();

            List<Tuple2<String, Integer>> outExpected = new ArrayList<Tuple2<String, Integer>>();
            outExpected.add(Tuple2.of("this", 3));
            outExpected.add(Tuple2.of("is", 2));
            outExpected.add(Tuple2.of("a", 2));
            outExpected.add(Tuple2.of("sentence", 2));
            outExpected.add(Tuple2.of("must", 1));
            outExpected.add(Tuple2.of("be", 1));
            outExpected.add(Tuple2.of("split", 2));
            outExpected.add(Tuple2.of("because", 1));
            outExpected.add(Tuple2.of("flatmap", 1));

            Thread.sleep(2000);
            // verify your results
            assertTrue(CollectSink.values.containsAll(outExpected));
        }
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<Tuple2<String, Integer>> {

        // must be static
        public static final List<Tuple2<String, Integer>> values = new ArrayList<>();

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            values.add(value);
        }
    }
}

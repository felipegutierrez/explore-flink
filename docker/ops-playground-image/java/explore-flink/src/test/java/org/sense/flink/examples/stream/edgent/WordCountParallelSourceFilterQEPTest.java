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
import org.sense.flink.source.WordsParallelSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.*;

import static org.junit.Assert.assertTrue;

public class WordCountParallelSourceFilterQEPTest {

    public static List<Tuple2<String, Integer>> totalTuplesFound;
    public static List<Tuple2<String, Integer>> totalTuplesExpected;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster;
    private final int minAvailableProcessors = 4;
    private final boolean runInParallel;

    public WordCountParallelSourceFilterQEPTest() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        this.runInParallel = availableProcessors >= this.minAvailableProcessors;
        if (this.runInParallel) {
            flinkCluster = new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(this.minAvailableProcessors)
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
            totalTuplesFound = Collections.synchronizedList(new ArrayList<Tuple2<String, Integer>>());
            totalTuplesExpected = Collections.synchronizedList(new ArrayList<Tuple2<String, Integer>>());

            // source file with multiple words
            String filePath = "/hamlet.txt";
            // String filePath = "/shorttext.txt";

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            // configure your test environment
            env.setParallelism(this.minAvailableProcessors);

            // values are collected in a static variable
            // CollectSink.values.clear();

            DataStream<Tuple2<String, Integer>> dataStream = env
                    .addSource(new WordsParallelSource(filePath, 1))
                    .flatMap(new WordCountParallelSourceFilterQEP.SplitterFlatMap())
                    .keyBy(new WordCountParallelSourceFilterQEP.WordKeySelector())
                    .reduce(new WordCountParallelSourceFilterQEP.SumReducer());
            dataStream.addSink(new CollectSink());

            String executionPlan = env.getExecutionPlan();
            System.out.println("ExecutionPlan ........................ ");
            System.out.println(executionPlan);
            System.out.println("........................ ");

            // execute
            env.execute();

            // expected values
            countWordsSequentially(filePath);

            // verify your results
            // assertTrue( CollectSink.values.containsAll(outExpected));
            assertTrue(totalTuplesFound.containsAll(totalTuplesExpected));
        }
    }

    private void countWordsSequentially(String filePath) throws IOException, URISyntaxException {
        Map<String, Integer> total = new HashMap<String, Integer>();
        InputStream is = getClass().getResourceAsStream(filePath);
        if (is == null) {
            System.err.println("Could not load file [" + filePath + "].");
        } else {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                String line;
                while ((line = br.readLine()) != null) {
                    // System.out.println(line);
                    String[] words = line.split(" ");
                    for (String w : words) {
                        if (total.containsKey(w)) {
                            Integer value = total.get(w) + 1;
                            total.put(w, value);
                        } else {
                            total.put(w, 1);
                        }
                    }
                }
            }
        }
        for (Map.Entry<String, Integer> entry : total.entrySet()) {
            // System.out.println(entry.getKey() + "," + entry.getValue());
            totalTuplesExpected.add(Tuple2.of(entry.getKey(), entry.getValue()));
        }
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<Tuple2<String, Integer>> {
        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            totalTuplesFound.add(value);
        }
    }
}

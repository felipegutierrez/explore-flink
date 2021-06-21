package org.sense.flink.examples.stream.operator.watermark;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.OutputTag;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.sense.flink.examples.stream.edgent.ExceptionSimulatorProcess;
import org.sense.flink.examples.stream.edgent.WordCountFilterQEP;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.sense.flink.examples.stream.edgent.ExceptionSimulatorProcess.POISON_TRANSACTION_ID;

public class WatermarkStreamOperatorTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster;
    private final int minAvailableProcessors = 4;
    private final boolean runInParallel;

    public WatermarkStreamOperatorTest() {
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
    public void testProcessKeepLatestWatermark() throws Exception {
        WatermarkStreamOperator<Integer> operator = new WatermarkStreamOperator<Integer>(new MyWatermarkFunc());

        OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
                new OneInputStreamOperatorTestHarness<Integer, Integer>(operator);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();

        testHarness.open();

        testHarness.processElement(new StreamRecord<Integer>(1, initialTime + 1));
        testHarness.processElement(new StreamRecord<Integer>(2, initialTime + 2));
        testHarness.processWatermark(new Watermark(initialTime + 2));
        testHarness.processElement(new StreamRecord<Integer>(3, initialTime + 3));
        testHarness.processElement(new StreamRecord<Integer>(4, initialTime + 4));
        testHarness.processElement(new StreamRecord<Integer>(5, initialTime + 5));
        testHarness.processWatermark(new Watermark(initialTime + 5));
        testHarness.processElement(new StreamRecord<Integer>(6, initialTime + 6));
        testHarness.processElement(new StreamRecord<Integer>(7, initialTime + 7));

        expectedOutput.add(new StreamRecord<Integer>(1, initialTime + 1));
        expectedOutput.add(new StreamRecord<Integer>(2, initialTime + 2));
        expectedOutput.add(new Watermark(initialTime + 2));
        expectedOutput.add(new StreamRecord<Integer>(3, initialTime + 3));
        expectedOutput.add(new StreamRecord<Integer>(4, initialTime + 4));
        expectedOutput.add(new StreamRecord<Integer>(5, initialTime + 5));
        expectedOutput.add(new Watermark(initialTime + 5));
        expectedOutput.add(new StreamRecord<Integer>(6, initialTime + 6));
        expectedOutput.add(new StreamRecord<Integer>(7, initialTime + 7));

        TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        assertEquals(5, testHarness.getCurrentWatermark());
    }

    @Test
    public void testOpenClose() throws Exception {
        WatermarkStreamOperator<String> operator = new WatermarkStreamOperator<String>(new TestOpenCloseWatermarkFunction());

        OneInputStreamOperatorTestHarness<String, String> testHarness =
                new OneInputStreamOperatorTestHarness<String, String>(operator);

        long initialTime = 0L;

        testHarness.open();

        testHarness.processElement(new StreamRecord<String>("fooHello", initialTime));
        testHarness.processElement(new StreamRecord<String>("bar", initialTime));

        testHarness.close();

        Assert.assertTrue("RichFunction methods where not called.", TestOpenCloseWatermarkFunction.closeCalled);
        Assert.assertTrue("Output contains no elements.", testHarness.getOutput().size() > 0);
        assertEquals(0, testHarness.getCurrentWatermark());
    }

    @Test
    public void testRestartWithLatestWatermark() throws Exception {

        String sentence = "this is a sentence " + POISON_TRANSACTION_ID;

        // expected values
        List<Tuple2<String, Integer>> outExpected = new ArrayList<Tuple2<String, Integer>>();
        outExpected.add(Tuple2.of("this", 1));
        outExpected.add(Tuple2.of("is", 1));
        outExpected.add(Tuple2.of("a", 1));
        outExpected.add(Tuple2.of("sentence", 1));
        outExpected.add(Tuple2.of(POISON_TRANSACTION_ID, 1));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start a checkpoint every 500 ms
        env.enableCheckpointing(500);
        // make sure 250 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(0);
        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // sets the checkpoint storage where checkpoint snapshots will be written
        env.setStateBackend(new HashMapStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage(new JobManagerStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage("file:///tmp/explore-flink/checkpoint");
        // env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///tmp/explore-flink/checkpoint"));

        env.getConfig().setLatencyTrackingInterval(5L);

        // configure your test environment
        env.setParallelism(this.minAvailableProcessors);

        // values are collected in a static variable
        CollectSink.values.clear();
        SideOutputSink.result.set(0L);

        // create a stream of custom elements and apply transformations
        DataStream<Tuple2<String, Integer>> dataStream = env
                .addSource(new MySource(sentence))
                .transform("myStatefulWatermarkOperator",
                        TypeInformation.of(String.class),
                        new WatermarkStreamOperator<>(new MyTupleWatermarkFunc()))
                .flatMap(new WordCountFilterQEP.SplitterFlatMap())
                .keyBy(new WordCountFilterQEP.WordKeySelector()) // select the first value as a key
                .reduce(new WordCountFilterQEP.SumReducer()) // reduce to sum all values with same key
                ;
        dataStream.addSink(new CollectSink());

        OutputTag<Long> outputTag = new OutputTag<Long>("side-output") {
        };
        dataStream
                .process(new ExceptionSimulatorProcess(1_000_000L, 5_000L)).name("exception-simulator")
                .getSideOutput(outputTag)
                .addSink(new SideOutputSink());


        String executionPlan = env.getExecutionPlan();
        System.out.println(executionPlan);

        // execute
        env.execute();

        // verify your results
        List<Tuple2<String, Integer>> currentValues = CollectSink.values;
        assertTrue(currentValues.containsAll(outExpected));

        // count how many times the job restarted
        assertTrue(SideOutputSink.result.get() > 0);
    }

    // This must only be used in one test, otherwise the static fields will be changed
    // by several tests concurrently
    private static class TestOpenCloseWatermarkFunction extends RichWatermarkFunction<String> {
        private static final long serialVersionUID = 1L;

        public static boolean openCalled = false;
        public static boolean closeCalled = false;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            if (closeCalled) {
                Assert.fail("Close called before open.");
            }
            openCalled = true;
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (!openCalled) {
                Assert.fail("Open was not called before close.");
            }
            closeCalled = true;
        }

        @Override
        public String process(String value) throws Exception {
            if (!openCalled) {
                Assert.fail("Open was not called before run.");
            }
            return value;
        }
    }

    static class MyWatermarkFunc implements WatermarkFunction<Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer process(Integer value) throws Exception {
            return value;
        }
    }

    static class MyTupleWatermarkFunc implements WatermarkFunction<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String process(String value) throws Exception {
            return value;
        }
    }

    private static class CollectSink implements SinkFunction<Tuple2<String, Integer>> {
        public static final List<Tuple2<String, Integer>> values = Collections.synchronizedList(new ArrayList<Tuple2<String, Integer>>());

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            values.add(value);
        }
    }

    private static class SideOutputSink implements SinkFunction<Long> {
        public static final AtomicLong result = new AtomicLong(0L);

        @Override
        public void invoke(Long value, Context context) throws Exception {
            result.set(value);
        }
    }

    private static class MySource implements SourceFunction<String> {
        private final String sentence;

        public MySource(String sentence) {
            this.sentence = sentence;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            String[] words = this.sentence.split(" ");
            for (int i = 0; i < words.length; i++) {
                Thread.sleep(250);
                ctx.collect(words[i]);
                if (i < 4) {
                    ctx.emitWatermark(new Watermark(i));
                }
            }
        }

        @Override
        public void cancel() {

        }
    }
}
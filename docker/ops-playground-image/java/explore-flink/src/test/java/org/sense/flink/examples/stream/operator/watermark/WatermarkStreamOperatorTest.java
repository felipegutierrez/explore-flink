package org.sense.flink.examples.stream.operator.watermark;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

public class WatermarkStreamOperatorTest {

    @Test
    public void testMapWatermark() throws Exception {
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

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
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
}
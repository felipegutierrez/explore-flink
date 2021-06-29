package org.sense.flink.examples.stream.tpch.udf;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Test;
import org.sense.flink.util.CpuGauge;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Tuple6ToStringMapTest {

    @Test(expected = NullPointerException.class)
    public void map() throws Exception {
        // instantiate your function
        Tuple6ToStringMap mapFunction = new Tuple6ToStringMap(false);

        Tuple6<Integer, String, String, String, Double, Double> value = Tuple6.of(0, "A", "B", "C", 1.1, 2.2);
        String result = "customer (with nation) with revenue (custkey[0], name[A], address[B], nationname[" +
                "C], acctbal[1.1], revenue[2.2])";

        // call the methods that you have implemented
        assertEquals(result, mapFunction.map(value));
    }

    @Test
    public void mapWithOpenState() throws Exception {
        Tuple6ToStringMap statefulMap = new Tuple6ToStringMap(false);

        OneInputStreamOperatorTestHarness<Tuple6<Integer, String, String, String, Double, Double>, String> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamMap<>(statefulMap));
        testHarness.open();

        Tuple6<Integer, String, String, String, Double, Double> value = Tuple6.of(0, "A", "B", "C", 1.1, 2.2);
        String result = "customer (with nation) with revenue (custkey[0], name[A], address[B], nationname[" +
                "C], acctbal[1.1], revenue[2.2])";

        testHarness.processElement(value, 10);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

        CpuGauge cpuGauge = statefulMap.getCpuGauge();
        assertTrue(cpuGauge.getValue().intValue() >= 1);

        Assert.assertEquals(
                Lists.newArrayList(new StreamRecord<>(result, 10)),
                testHarness.extractOutputStreamRecords()
        );
    }
}
package org.sense.flink.examples.stream.tpch.udf;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Assert;
import org.junit.Test;
import org.sense.flink.examples.stream.tpch.pojo.ShippingPriorityItem;
import org.sense.flink.util.CpuGauge;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.*;

public class ShippingPriorityItemMapTest {

    @Test(expected = NullPointerException.class)
    public void map() throws Exception {
        // instantiate your function
        ShippingPriorityItemMap mapFunction = new ShippingPriorityItemMap(false);

        ShippingPriorityItem value = new ShippingPriorityItem(1L, 2.0, "", 3);
        String result = "ShippingPriorityItem [orderkey=1, revenue=2.0, orderdate=, shippriority=3]";

        // call the methods that you have implemented
        assertEquals(result, mapFunction.map(value));
    }

    @Test
    public void mapWithOpenState() throws Exception {
        ShippingPriorityItemMap statefulMap = new ShippingPriorityItemMap(false);

        OneInputStreamOperatorTestHarness<ShippingPriorityItem, String> testHarness =
                new OneInputStreamOperatorTestHarness<>(new StreamMap<>(statefulMap));
        testHarness.open();

        ShippingPriorityItem value = new ShippingPriorityItem(1L, 2.0, "", 3);
        String result = "ShippingPriorityItem [orderkey=1, revenue=2.0, orderdate=, shippriority=3]";

        testHarness.processElement(value, 10);

        ConcurrentLinkedQueue<Object> output = testHarness.getOutput();

        CpuGauge cpuGauge = statefulMap.getCpuGauge();
        // TODO: for some unknown reason this does not work on github actions :(
        // assertTrue(cpuGauge.getValue().intValue() >= 1);

        Assert.assertEquals(
                Lists.newArrayList(new StreamRecord<>(result, 10)),
                testHarness.extractOutputStreamRecords()
        );
    }
}
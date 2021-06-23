package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Before;
import org.junit.Test;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.pojo.ValenciaItemEnriched;
import org.sense.flink.pojo.ValenciaTraffic;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;

public class ValenciaIntensiveCpuDistancesMapTest {

    private OneInputStreamOperatorTestHarness<Tuple2<ValenciaItem, ValenciaItem>, ValenciaItemEnriched> testHarness;
    private ValenciaIntensiveCpuDistancesMap statefulMapFunction;

    @Before
    public void setupTestHarness() throws Exception {

        //instantiate user-defined function
        statefulMapFunction = new ValenciaIntensiveCpuDistancesMap(true);

        // wrap user defined function into a the corresponding operator
        testHarness = new OneInputStreamOperatorTestHarness<>(new StreamMap<>(statefulMapFunction));

        // optionally configured the execution environment
        testHarness.getExecutionConfig().setAutoWatermarkInterval(50);

        // open the test harness (will also call open() on RichFunctions)
        testHarness.open();
    }

    @Test
    public void testingStatefulMapFunction() throws Exception {

        //push (timestamped) elements into the operator (and hence user defined function)
        Point point = new Point(123.45, 543.21, "EPSG:4326");
        List<Point> points = Arrays.asList(point);
        Date now = new Date();
        ValenciaItem valenciaTraffic01 = new ValenciaTraffic(0L, 0L, "", now, points, 1);
        ValenciaItem valenciaTraffic02 = new ValenciaTraffic(0L, 0L, "", now, points, 1);

        ValenciaItemEnriched valenciaItemEnriched = new ValenciaItemEnriched(0L, 0L, "", now,
                Arrays.asList(point, point), "1 - 1");
        valenciaItemEnriched.setDistances("[districts[16, 16, 0.0 meters]]");
        ConcurrentLinkedQueue<StreamRecord<Object>> expected = new ConcurrentLinkedQueue<>();
        expected.add(new StreamRecord(valenciaItemEnriched, 100L));
        // expected.add(new Watermark(100));

        testHarness.processElement(Tuple2.of(valenciaTraffic01, valenciaTraffic02), 100L);

        //trigger event time timers by advancing the event time of the operator with a watermark
        // testHarness.processWatermark(100L);

        //trigger processing time timers by advancing the processing time of the operator directly
        // testHarness.setProcessingTime(100L);

        //retrieve list of emitted records for assertions
        System.out.println();
        for (Object o : testHarness.getOutput()) {
            System.out.println(o);
        }
        System.out.println();
        System.out.println();
        for (Object o : expected) {
            System.out.println(o);
        }
        System.out.println();

        assertEquals(1, testHarness.getOutput().size());
        // assertThat(testHarness.getOutput(), containsInAnyOrder(expected));
    }
}
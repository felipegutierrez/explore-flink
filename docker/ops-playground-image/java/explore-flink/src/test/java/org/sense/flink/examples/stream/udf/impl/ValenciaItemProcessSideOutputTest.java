package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.Test;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.pojo.ValenciaTraffic;
import org.sense.flink.util.CRSCoordinateTransformer;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.sense.flink.util.CRSCoordinateTransformer.DEFAULT_CRS_EPSG_4326;

public class ValenciaItemProcessSideOutputTest {

    @Test
    public void testProcessElement() throws Exception {
        //instantiate user-defined function
        ValenciaItemProcessSideOutput processFunction = new ValenciaItemProcessSideOutput(
                new ValenciaItemOutputTag("side-output-traffic"));

        // wrap user defined function into a the corresponding operator
        OneInputStreamOperatorTestHarness<ValenciaItem, ValenciaItem> harness = ProcessFunctionTestHarnesses
                .forProcessFunction(processFunction);


        List<Point> points = new ArrayList<Point>();
        CRSCoordinateTransformer ct = new CRSCoordinateTransformer();
        double[] lonLat = ct.xyToLonLat(728630.3849999998, 4370921.0409);
        /*points.add(new Point(lonLat[0], lonLat[1], DEFAULT_CRS_EPSG_4326));
        ValenciaItem valenciaItem = new ValenciaTraffic(0L, 0L, "", new Date(), points,
                (new Random().nextInt((100 - 1) + 1) + 1));
        //push (timestamped) elements into the operator (and hence user defined function)
        harness.processElement(valenciaItem, 10);

        //retrieve list of emitted records for assertions
        assertEquals(harness.extractOutputValues(), Collections.singletonList(1));*/
    }
}
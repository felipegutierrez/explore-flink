package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;
import org.sense.flink.examples.stream.valencia.ValenciaDataSkewedJoinExample;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.pojo.ValenciaPollution;
import org.sense.flink.pojo.ValenciaTraffic;
import org.sense.flink.util.ValenciaItemType;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

public class ValenciaItemSyntheticDataTest {

    @Test
    public void flatMapTrafficJam() throws Exception {
        // instantiate your function
        List<Tuple4<Point, Long, Long, String>> coordinates = ValenciaDataSkewedJoinExample.syntheticCoordinates();
        ValenciaItemSyntheticData incrementer = new ValenciaItemSyntheticData(ValenciaItemType.TRAFFIC_JAM, coordinates);

        Collector<ValenciaItem> collector = mock(Collector.class);

        List<Point> points = Arrays.asList(new Point(123.45, 543.21));
        ValenciaItem valenciaItem = new ValenciaTraffic(0L, 0L, "", new Date(), points, 1);

        // call the methods that you have implemented
        incrementer.flatMap(valenciaItem, collector);

        //verify collector was called with the right output
        Mockito.verify(collector, times(1)).collect(valenciaItem);
    }

    @Test
    public void flatMapAirPollution() throws Exception {
        // instantiate your function
        List<Tuple4<Point, Long, Long, String>> coordinates = ValenciaDataSkewedJoinExample.syntheticCoordinates();
        ValenciaItemSyntheticData incrementer = new ValenciaItemSyntheticData(ValenciaItemType.AIR_POLLUTION, coordinates);

        Collector<ValenciaItem> collector = mock(Collector.class);

        List<Point> points = Arrays.asList(new Point(123.45, 543.21));
        ValenciaItem valenciaItem01 = new ValenciaPollution(0L, 0L, "", new Date(), points, "1");
        ValenciaItem valenciaItem02 = new ValenciaPollution(0L, 0L, "", new Date(), points, "2");
        ValenciaItem valenciaItem03 = new ValenciaPollution(0L, 0L, "", new Date(), points, "3");

        // call the methods that you have implemented
        incrementer.flatMap(valenciaItem01, collector);
        incrementer.flatMap(valenciaItem02, collector);
        incrementer.flatMap(valenciaItem03, collector);

        //verify collector was called with the right output
        Mockito.verify(collector, times(1)).collect(valenciaItem01);
        Mockito.verify(collector, times(1)).collect(valenciaItem02);
        Mockito.verify(collector, times(1)).collect(valenciaItem03);
    }
}
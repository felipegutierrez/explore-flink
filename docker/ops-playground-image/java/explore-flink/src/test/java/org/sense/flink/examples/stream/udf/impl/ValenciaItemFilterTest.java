package org.sense.flink.examples.stream.udf.impl;

import org.junit.Test;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.pojo.ValenciaPollution;
import org.sense.flink.pojo.ValenciaTraffic;
import org.sense.flink.util.ValenciaItemType;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ValenciaItemFilterTest {

    @Test
    public void testTrafficFilter() throws Exception {
        // instantiate your function
        ValenciaItemFilter filter = new ValenciaItemFilter(ValenciaItemType.TRAFFIC_JAM);

        List<Point> points = Arrays.asList(new Point(123.45, 543.21));
        ValenciaItem valenciaItem = new ValenciaTraffic(0L, 0L, "", new Date(), points, 1);

        // call the methods that you have implemented
        assertEquals(true, filter.filter(valenciaItem));
    }

    @Test
    public void testAirPollutionFilter() throws Exception {
        // instantiate your function
        ValenciaItemFilter filter = new ValenciaItemFilter(ValenciaItemType.AIR_POLLUTION);

        List<Point> points = Arrays.asList(new Point(123.45, 543.21));
        ValenciaItem valenciaItem = new ValenciaPollution(0L, 0L, "", new Date(), points, "1");

        // call the methods that you have implemented
        assertEquals(true, filter.filter(valenciaItem));
    }

    @Test(expected = ClassCastException.class)
    public void testAirPollutionFilterException() throws Exception {
        // instantiate your function
        ValenciaItemFilter filter = new ValenciaItemFilter(ValenciaItemType.AIR_POLLUTION);

        List<Point> points = Arrays.asList(new Point(123.45, 543.21));
        ValenciaItem valenciaItem = new ValenciaPollution(0L, 0L, "", new Date(), points, 1);
    }

    @Test
    public void testNoiseFilter() throws Exception {
        // instantiate your function
        ValenciaItemFilter filter = new ValenciaItemFilter(ValenciaItemType.NOISE);

        List<Point> points = Arrays.asList(new Point(123.45, 543.21));
        ValenciaItem valenciaItem = new ValenciaTraffic(0L, 0L, "", new Date(), points, 1);

        // call the methods that you have implemented
        // NOISE filter is not implemented yet
        assertEquals(false, filter.filter(valenciaItem));
    }

    @Test(expected = Exception.class)
    public void testNullFilter() throws Exception {
        // instantiate your function
        ValenciaItemFilter filter = new ValenciaItemFilter(null);

        List<Point> points = Arrays.asList(new Point(123.45, 543.21));
        ValenciaItem valenciaItem = new ValenciaTraffic(0L, 0L, "", new Date(), points, 1);

        // call the methods that you have implemented
        assertEquals(true, filter.filter(valenciaItem));
    }
}
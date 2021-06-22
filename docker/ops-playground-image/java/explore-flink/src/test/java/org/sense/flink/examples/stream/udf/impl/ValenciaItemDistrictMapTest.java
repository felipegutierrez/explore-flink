package org.sense.flink.examples.stream.udf.impl;

import org.junit.Test;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.pojo.ValenciaPollution;
import org.sense.flink.pojo.ValenciaTraffic;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ValenciaItemDistrictMapTest {

    @Test
    public void mapTraffic() throws Exception {
        // instantiate your function
        ValenciaItemDistrictMap mapFunction = new ValenciaItemDistrictMap();

        List<Point> points = Arrays.asList(new Point(123.45, 543.21, "EPSG:4326"));
        ValenciaItem valenciaItem = new ValenciaTraffic(0L, 0L, "", new Date(), points, 1);

        // call the methods that you have implemented
        assertEquals(valenciaItem, mapFunction.map(valenciaItem));
    }

    @Test
    public void mapAirPollution() throws Exception {
        // instantiate your function
        ValenciaItemDistrictMap mapFunction = new ValenciaItemDistrictMap();

        List<Point> points = Arrays.asList(new Point(123.45, 543.21, "EPSG:4326"));
        ValenciaItem valenciaItem = new ValenciaPollution(0L, 0L, "", new Date(), points, "1");

        // call the methods that you have implemented
        assertEquals(valenciaItem, mapFunction.map(valenciaItem));
    }

    @Test(expected = ClassCastException.class)
    public void testAirPollutionMapException() throws Exception {
        // instantiate your function
        ValenciaItemDistrictMap mapFunction = new ValenciaItemDistrictMap();

        List<Point> points = Arrays.asList(new Point(123.45, 543.21));
        ValenciaItem valenciaItem = new ValenciaPollution(0L, 0L, "", new Date(), points, 1);
    }
}
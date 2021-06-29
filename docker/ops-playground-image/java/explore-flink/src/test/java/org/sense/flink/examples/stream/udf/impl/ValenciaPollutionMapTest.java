package org.sense.flink.examples.stream.udf.impl;

import org.junit.Test;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.pojo.ValenciaPollution;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class ValenciaPollutionMapTest {

    @Test
    public void map() throws Exception {
        // instantiate your function
        ValenciaPollutionMap mapFunction = new ValenciaPollutionMap();

        List<Point> points = Arrays.asList(new Point(123.45, 543.21, "EPSG:4326"));
        ValenciaItem value = new ValenciaPollution(0L, 0L, "", new Date(), points, "http://mapas.valencia.es/WebsMunicipales/uploads/atmosferica/6A.csv");

        // call the methods that you have implemented
        ValenciaItem result = mapFunction.map(value);

        assertTrue(result instanceof ValenciaPollution);
    }
}
package org.sense.flink.examples.stream.udf.impl;

import org.junit.Test;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.pojo.ValenciaPollution;
import org.sense.flink.pojo.ValenciaTraffic;
import org.sense.flink.util.TrafficStatus;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TrafficPollutionByDistrictJoinFunctionTest {

    @Test
    public void join() throws Exception {
        TrafficPollutionByDistrictJoinFunction joinFunction = new TrafficPollutionByDistrictJoinFunction();

        Point point = new Point(123.45, 543.21, "EPSG:4326");
        List<Point> points = Arrays.asList(point);
        Date now = new Date();
        ValenciaItem traffic = new ValenciaTraffic(0L, 0L, "", now, points, 1);

        ValenciaItem pollution = new ValenciaPollution(0L, 0L, "", new Date(), points, "http://mapas.valencia.es/WebsMunicipales/uploads/atmosferica/6A.csv");

        String result = joinFunction.join(traffic, pollution);
        String expected = "0  0 Traffic[" + TrafficStatus.getStatus((Integer) traffic.getValue()) + "] " + pollution.getValue();
        assertEquals(expected, result);
    }
}

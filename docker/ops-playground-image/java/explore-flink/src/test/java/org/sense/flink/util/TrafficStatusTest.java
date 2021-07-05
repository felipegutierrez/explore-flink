package org.sense.flink.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.sense.flink.util.TrafficStatus.*;

public class TrafficStatusTest {

    @Test
    public void getStatus() {
        String trafficStatusFlowing = TrafficStatus.getStatus(STATUS_FLOWING);
        String trafficStatusDense = TrafficStatus.getStatus(STATUS_DENSE);
        String trafficStatusCongested = TrafficStatus.getStatus(STATUS_CONGESTED);
        String trafficStatusChopped = TrafficStatus.getStatus(STATUS_CHOPPED);
        String trafficStatusNull = TrafficStatus.getStatus(null);
        String trafficStatusEmpty = TrafficStatus.getStatus(4);

        assertEquals("flowing", trafficStatusFlowing);
        assertEquals("dense", trafficStatusDense);
        assertEquals("congested", trafficStatusCongested);
        assertEquals("chopped up", trafficStatusChopped);
        assertEquals("", trafficStatusNull);
        assertEquals("", trafficStatusEmpty);
    }
}

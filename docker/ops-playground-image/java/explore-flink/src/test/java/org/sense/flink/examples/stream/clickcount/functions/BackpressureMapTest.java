package org.sense.flink.examples.stream.clickcount.functions;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.sense.flink.examples.stream.clickcount.records.ClickEvent;

import java.util.Date;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class BackpressureMapTest {

    @Mock
    BackpressureMap backpressureMap;

    @Test
    public void testSumMapper() throws Exception {

        long start = System.currentTimeMillis();

        Mockito.when(backpressureMap.causeBackpressure()).thenReturn(true);
        Mockito.when(backpressureMap.map(any())).thenCallRealMethod();

        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));
        backpressureMap.map(new ClickEvent(new Date(System.currentTimeMillis()), "page"));

        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed > 1000);
    }
}
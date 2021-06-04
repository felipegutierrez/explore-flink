package org.sense.flink.examples.stream.clickcount.functions;

import org.junit.Test;
import org.sense.flink.examples.stream.clickcount.records.ClickEvent;

import static org.junit.Assert.assertEquals;

public class CountingAggregatorTest {

    CountingAggregator countingAggregator = new CountingAggregator();

    @Test
    public void testCreateAccumulator() {
        assertEquals(Long.valueOf(0), countingAggregator.createAccumulator());
    }

    @Test
    public void testAdd() {
        Long accumulator = 0L;
        ClickEvent clickEvent = new ClickEvent();

        accumulator = countingAggregator.add(clickEvent, accumulator);
        assertEquals(Long.valueOf(1), accumulator);

        accumulator = countingAggregator.add(clickEvent, accumulator);
        assertEquals(Long.valueOf(2), accumulator);

        accumulator = countingAggregator.add(clickEvent, accumulator);
        assertEquals(Long.valueOf(3), accumulator);
    }

    @Test
    public void testMerge() {
        assertEquals(Long.valueOf(5), countingAggregator.merge(3L, 2L));
    }

    @Test
    public void testGetResult() {
        assertEquals(Long.valueOf(5), countingAggregator.getResult(5L));
    }
}
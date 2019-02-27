package org.sense.flink.util;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

public class CountMinSketchTest {

	@Test
	public void testSketch() throws InterruptedException, ExecutionException, TimeoutException {

		CountMinSketch countMinSketch = new CountMinSketch();

		Boolean future01 = countMinSketch.updateSketch("Felipe");
		Boolean future02 = countMinSketch.updateSketch("Oliveira");
		Boolean future03 = countMinSketch.updateSketch("Gutierrez");
		Boolean future04 = countMinSketch.updateSketch("Felipe");

		int expected;
		int actual;

		expected = 0;
		actual = countMinSketch.getFrequencyFromSketch("de");
		assertEquals(expected, actual);

		if (future02.equals(true)) {
			expected = 1;
			actual = countMinSketch.getFrequencyFromSketch("Oliveira");
			assertEquals(expected, actual);
		}

		if (future04.equals(true)) {
			expected = 2;
			actual = countMinSketch.getFrequencyFromSketch("Felipe");
			assertEquals(expected, actual);
		}
	}
}

package org.sense.flink.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class CountMinSketchTest extends TestCase {

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
		// assertTrue(true);
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

		future01 = countMinSketch.updateSketch("Felipe");
		future02 = countMinSketch.updateSketch("Oliveira");
		future03 = countMinSketch.updateSketch("Gutierrez");
		future04 = countMinSketch.updateSketch("Felipe");

		Thread.sleep(1000);

		expected = 0;
		actual = countMinSketch.getFrequencyFromSketch("de");
		assertEquals(expected, actual);

		if (future02.equals(true)) {
			expected = 2;
			actual = countMinSketch.getFrequencyFromSketch("Oliveira");
			assertEquals(expected, actual);
		}

		if (future04.equals(true)) {
			expected = 4;
			actual = countMinSketch.getFrequencyFromSketch("Felipe");
			assertEquals(expected, actual);
		}
	}
}

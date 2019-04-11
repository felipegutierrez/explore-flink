package org.sense.flink.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

public class SkewParameterGeneratorTest extends TestCase {

	public void testSkewParameterGenerator() throws InterruptedException, ExecutionException, TimeoutException {

		SkewParameterGenerator skewParameterGenerator = new SkewParameterGenerator(5);

		Integer expected = 1;
		assertEquals(expected, skewParameterGenerator.getNextItem());
		expected = 2;
		assertEquals(expected, skewParameterGenerator.getNextItem());
		expected = 3;
		assertEquals(expected, skewParameterGenerator.getNextItem());
		expected = 4;
		assertEquals(expected, skewParameterGenerator.getNextItem());
		expected = 5;
		assertEquals(expected, skewParameterGenerator.getNextItem());
		expected = 1;
		assertEquals(expected, skewParameterGenerator.getNextItem());
	}
}

package org.sense.flink.pojo;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class PointTest extends TestCase {
	public PointTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(PointTest.class);
	}

	public void testDistance() {
		Point orig = new Point(729664.353, 4373064.801);
		Point dest = new Point(730021.116, 4373070.406);
		double d = orig.euclideanDistance(dest);
		assertEquals(356.807, d, 0.005);
	}
}

package org.sense.flink.util;

import java.io.File;

import org.sense.flink.pojo.Point;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class GeoJSONUtilTest extends TestCase {
	public GeoJSONUtilTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(GeoJSONUtilTest.class);
	}

	public void testIsInside() {
		File geoJSON = new File("resources/valencia/admin_level_9_Valencia_polygons.geojson");
		Point point = new Point(-0.3630, 39.4477);
		Boolean isInside = GeoJSONUtil.isInside(point, geoJSON);
		assertTrue(isInside);
	}

	public void testOsmId() {
		File geoJSON = new File("resources/valencia/admin_level_9_Valencia_polygons.geojson");
		Point point = new Point(-0.3630, 39.4477);
		String osmId = GeoJSONUtil.getOsmId(point, geoJSON);
		assertEquals("5767948", osmId);
	}
}

package org.sense.flink.util;

import java.io.File;

import org.apache.flink.api.java.tuple.Tuple3;
import org.sense.flink.pojo.Point;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class SimpleGeographicalPolygonsTest extends TestCase {
	public SimpleGeographicalPolygonsTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(SimpleGeographicalPolygonsTest.class);
	}

	public void testOsmId() throws Exception {
		SimpleGeographicalPolygons sgp = new SimpleGeographicalPolygons(
				new File("resources/valencia/admin_level_9_Valencia_polygons.geojson"));
		Long osmId = sgp.getOsmId(new Point(-0.3630, 39.4477, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_4326));
		assertEquals(Long.valueOf(5767948), osmId);

		osmId = sgp.getOsmId(new Point(-0.3774, 39.4698, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_4326));
		assertEquals(Long.valueOf(4231802), osmId);
	}

	public void testAdminLevel() throws Exception {
		SimpleGeographicalPolygons sgp = new SimpleGeographicalPolygons(
				new File("resources/valencia/admin_level_9_Valencia_polygons.geojson"));
		Tuple3<Long, Long, String> adminLevel = sgp
				.getAdminLevel(new Point(-0.3630, 39.4477, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_4326));
		assertEquals(Long.valueOf(9), adminLevel.f1);

		adminLevel = sgp.getAdminLevel(new Point(-0.3774, 39.4698, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_4326));
		assertEquals(Long.valueOf(9), adminLevel.f1);
	}

	public void testContains() throws Exception {
		SimpleGeographicalPolygons sgp = new SimpleGeographicalPolygons(
				new File("resources/valencia/admin_level_9_Valencia_polygons.geojson"));
		Boolean contains = sgp.contains(new Point(-0.3630, 39.4477, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_4326));
		assertEquals(Boolean.TRUE, contains);

		contains = sgp.contains(new Point(-0.3774, 39.4698, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_4326));
		assertEquals(Boolean.TRUE, contains);

		contains = sgp.contains(new Point(-46.625290, -23.533773, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_4326));
		assertEquals(Boolean.FALSE, contains);
	}
}

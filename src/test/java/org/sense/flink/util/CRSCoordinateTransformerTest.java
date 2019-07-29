package org.sense.flink.util;

import org.opengis.referencing.operation.TransformException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class CRSCoordinateTransformerTest extends TestCase {
	public CRSCoordinateTransformerTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(CRSCoordinateTransformerTest.class);
	}

	public void testXYtoLongitdeLatitude() {
		CRSCoordinateTransformer ct = new CRSCoordinateTransformer();
		double x = (double) 727883.536;
		double y = (double) 4373590.846;
		try {
			double[] lonLat = ct.xyToLonLat(x, y);
			// xyToLonLat: -0.350420237194358 39.48175131012853
			assertEquals(-0.35042, lonLat[0], 0.00005);
			assertEquals(39.48175, lonLat[1], 0.00005);
		} catch (TransformException e) {
			e.printStackTrace();
		}
	}

	public void testLongitdeLatitudeToXY() {
		CRSCoordinateTransformer ct = new CRSCoordinateTransformer();
		double lon = (double) -0.350420237194358;
		double lat = (double) 39.48175131012853;
		try {
			double[] xy = ct.lonLatToXY(lon, lat);
			// lonLatToXY: 727883.5357436275 4373590.854780695
			assertEquals(727883.53574, xy[0], 0.00005);
			assertEquals(4373590.85478, xy[1], 0.00005);
		} catch (TransformException e) {
			e.printStackTrace();
		}
	}
}

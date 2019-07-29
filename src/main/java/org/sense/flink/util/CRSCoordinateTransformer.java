package org.sense.flink.util;

import java.io.Serializable;

import org.geotools.geometry.DirectPosition2D;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.sense.flink.pojo.Point;

public class CRSCoordinateTransformer implements Serializable {
	private static final long serialVersionUID = -7588347384995665882L;
	public static final String DEFAULT_CRS_SOURCE = "EPSG:25830";
	public static final String DEFAULT_CRS_TARGET = "EPSG:4326";
	private MathTransform forwardMathTransform;
	private MathTransform reverseMathTransform;
	private CoordinateReferenceSystem sourceCoordinateReferenceSystem;
	private CoordinateReferenceSystem targetCoordinateReferenceSystem;

	public CRSCoordinateTransformer() {
		try {
			sourceCoordinateReferenceSystem = CRS.decode(DEFAULT_CRS_SOURCE);
			targetCoordinateReferenceSystem = CRS.decode(DEFAULT_CRS_TARGET);
			this.forwardMathTransform = CRS.findMathTransform(sourceCoordinateReferenceSystem,
					targetCoordinateReferenceSystem, true);
			this.reverseMathTransform = CRS.findMathTransform(targetCoordinateReferenceSystem,
					sourceCoordinateReferenceSystem, true);
		} catch (FactoryException fex) {
			throw new ExceptionInInitializerError(fex);
		}
	}

	public double[] lonLatToXY(double lon, double lat) throws TransformException {
		DirectPosition2D srcDirectPosition2D = new DirectPosition2D(sourceCoordinateReferenceSystem, lat, lon);
		DirectPosition2D destDirectPosition2D = new DirectPosition2D();
		try {
			reverseMathTransform.transform(srcDirectPosition2D, destDirectPosition2D);
			return new double[] { destDirectPosition2D.x, destDirectPosition2D.y };
		} catch (Error error) {
			throw error;
		}
	}

	public double[] xyToLonLat(double x, double y) throws TransformException {

		DirectPosition2D srcDirectPosition2D = new DirectPosition2D(sourceCoordinateReferenceSystem, x, y);
		DirectPosition2D destDirectPosition2D = new DirectPosition2D();
		forwardMathTransform.transform(srcDirectPosition2D, destDirectPosition2D);

		return new double[] { destDirectPosition2D.y, destDirectPosition2D.x };
	}

	public Point xyToLonLatPoint(double x, double y) throws TransformException {
		double[] lonLat = xyToLonLat(x, y);
		return new Point(lonLat[0], lonLat[1], DEFAULT_CRS_TARGET);
	}
}

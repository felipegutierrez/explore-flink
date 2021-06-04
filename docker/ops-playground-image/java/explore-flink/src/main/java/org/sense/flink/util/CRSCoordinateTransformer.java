package org.sense.flink.util;

import org.geotools.geometry.DirectPosition2D;
import org.geotools.geometry.DirectPosition3D;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.sense.flink.pojo.Point;

import java.io.Serializable;

/**
 * The IOGP’s EPSG Geodetic Parameter Dataset is a collection of definitions of
 * coordinate reference systems and coordinate transformations which may be
 * global, regional, national or local in application.
 * 
 * To date (29/07/2019), the open-data catalog of Valencia portal is publishing
 * coordinates using the format EPSG:25830 and we are converting it to the
 * format EPSG:4326. The latter format (EPSG:4326) is the same as the polygons
 * on the GeoJSON file from Valencia which has the districts (administrative
 * level 9): resources/valencia/admin_level_9_Valencia_polygons.geojson and it
 * can be translated using the website below.
 * 
 * https://epsg.io/transform#s_srs=25830&t_srs=4326&x=725704.3880000&y=4370895.1142000
 * 
 * @author felipe
 *
 */
public class CRSCoordinateTransformer implements Serializable {
	private static final long serialVersionUID = -7588347384995665882L;
	public static final String DEFAULT_CRS_EPSG_25830 = "EPSG:25830";
	public static final String DEFAULT_CRS_EPSG_4326 = "EPSG:4326";
	public static final String DEFAULT_CRS_EPSG_4989_RADIUS = "EPSG:4989";
	private MathTransform transformEPSG25830;
	private MathTransform transformEPSG4326;
	private MathTransform transformRadius;
	private CoordinateReferenceSystem coordinateReferenceSystemEPSG25830;
	private CoordinateReferenceSystem coordinateReferenceSystemEPSG4326;
	private CoordinateReferenceSystem coordinateReferenceSystemRadius;

	public CRSCoordinateTransformer() {
		try {
			coordinateReferenceSystemEPSG25830 = CRS.decode(DEFAULT_CRS_EPSG_25830);
			coordinateReferenceSystemEPSG4326 = CRS.decode(DEFAULT_CRS_EPSG_4326);
			coordinateReferenceSystemRadius = CRS.decode(DEFAULT_CRS_EPSG_4989_RADIUS);
			this.transformEPSG25830 = CRS.findMathTransform(coordinateReferenceSystemEPSG25830,
					coordinateReferenceSystemEPSG4326, true);
			this.transformEPSG4326 = CRS.findMathTransform(coordinateReferenceSystemEPSG4326,
					coordinateReferenceSystemEPSG25830, true);
			this.transformRadius = CRS.findMathTransform(coordinateReferenceSystemRadius,
					coordinateReferenceSystemRadius, true);
		} catch (FactoryException fex) {
			throw new ExceptionInInitializerError(fex);
		}
	}

	public double[] lonLatToXY(double lon, double lat) throws TransformException {
		DirectPosition2D srcDirectPosition2D = new DirectPosition2D(coordinateReferenceSystemEPSG25830, lat, lon);
		DirectPosition2D destDirectPosition2D = new DirectPosition2D();
		try {
			transformEPSG4326.transform(srcDirectPosition2D, destDirectPosition2D);
			return new double[] { destDirectPosition2D.x, destDirectPosition2D.y };
		} catch (Error error) {
			throw error;
		}
	}

	public double[] xyToLonLat(double x, double y) throws TransformException {

		DirectPosition2D srcDirectPosition2D = new DirectPosition2D(coordinateReferenceSystemEPSG25830, x, y);
		DirectPosition2D destDirectPosition2D = new DirectPosition2D();
		transformEPSG25830.transform(srcDirectPosition2D, destDirectPosition2D);

		return new double[] { destDirectPosition2D.y, destDirectPosition2D.x };
	}

	public Point xyToLonLatPoint(double x, double y) throws TransformException {
		double[] lonLat = xyToLonLat(x, y);
		return new Point(lonLat[0], lonLat[1], DEFAULT_CRS_EPSG_4326);
	}

	public double[] lonLatToRadius(double lon, double lat) throws TransformException {
		DirectPosition3D srcDirectPosition3D = new DirectPosition3D(coordinateReferenceSystemRadius, lat, lon, 0);
		DirectPosition3D destDirectPosition3D = new DirectPosition3D();
		try {
			transformRadius.transform(srcDirectPosition3D, destDirectPosition3D);
			return new double[] { destDirectPosition3D.x, destDirectPosition3D.y };
		} catch (Error error) {
			throw error;
		}
	}

	public Point lonLatToRadiusPoint(double x, double y) throws TransformException {
		double[] lonLat = lonLatToRadius(x, y);
		return new Point(lonLat[0], lonLat[1], DEFAULT_CRS_EPSG_4989_RADIUS);
	}

	public double[] xyToRadius(double x, double y) throws TransformException {
		DirectPosition3D srcDirectPosition3D = new DirectPosition3D(coordinateReferenceSystemRadius, x, y, 0);
		DirectPosition3D destDirectPosition3D = new DirectPosition3D();
		transformRadius.transform(srcDirectPosition3D, destDirectPosition3D);

		return new double[] { destDirectPosition3D.y, destDirectPosition3D.x };
	}

	public Point xyToRadiusPoint(double x, double y) throws TransformException {
		double[] lonLat = xyToRadius(x, y);
		return new Point(lonLat[0], lonLat[1], DEFAULT_CRS_EPSG_4989_RADIUS);
	}

	public static void main(String[] args) {
		try {
			// @formatter:off
			// Point(725704.3880000003 4370895.1142)
			CRSCoordinateTransformer ct = new CRSCoordinateTransformer();
			double[] lonLat = ct.xyToLonLat(725704.3880000003, 4370895.1142);
			if (lonLat != null) System.out.println(lonLat[0] + " " + lonLat[1]);

			Point lonLatPoint = ct.xyToLonLatPoint(725704.3880000003, 4370895.1142);
			System.out.println(lonLatPoint.getX() + " " + lonLatPoint.getY());

			System.out.println("\nConverting coordinates: 728630.3849999998 4370921.0409 ----");
			lonLat = ct.xyToLonLat(728630.3849999998, 4370921.0409);
			System.out.println(lonLat[0] + " " + lonLat[1]);
			double[] xy = ct.lonLatToXY(lonLat[0], lonLat[1]);
			System.out.println(xy[0] + " " + xy[1]);

			System.out.println("\nConverting coordinates: 725140.37, 4371855.492 ----");
			lonLat = ct.xyToLonLat(725140.37, 4371855.492);
			System.out.println(lonLat[0] + " " + lonLat[1]);
			xy = ct.lonLatToXY(lonLat[0], lonLat[1]);
			System.out.println(xy[0] + " " + xy[1]);

			System.out.println("\nConverting coordinates: 724034.3761, 4369995.1312 ---- -0.3963375643709434 39.45039604501942");
			lonLat = ct.xyToLonLat(724034.3761, 4369995.1312);
			System.out.println(lonLat[0] + " " + lonLat[1]);
			xy = ct.lonLatToXY(lonLat[0], lonLat[1]);
			System.out.println(xy[0] + " " + xy[1]);

			System.out.println("\nConverting coordinates: 726777.707, 4369824.436 ---- -0.36454475732992986 39.448141729595456");
			lonLat = ct.xyToLonLat(726777.707, 4369824.436);
			System.out.println(lonLat[0] + " " + lonLat[1]);
			xy = ct.lonLatToXY(lonLat[0], lonLat[1]);
			System.out.println(xy[0] + " " + xy[1]);
			
			System.out.println("\nConverting radius: 726777.707, 4369824.436 ---- -0.36454475732992986 39.448141729595456");
			lonLat = ct.xyToRadius(726777.707, 4369824.436);
			System.out.println(lonLat[0] + " " + lonLat[1]);
			xy = ct.lonLatToRadius(lonLat[0], lonLat[1]);
			System.out.println(xy[0] + " " + xy[1]);
			// @formatter:on
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

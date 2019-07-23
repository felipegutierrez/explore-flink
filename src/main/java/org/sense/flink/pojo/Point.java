package org.sense.flink.pojo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.geometry.DirectPosition;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import com.vividsolutions.jts.geom.Coordinate;

public class Point implements Serializable {
	private static final long serialVersionUID = -5344602209568822697L;
	private Double x;
	private Double y;
	private Coordinate coordinates;

	public Point(Double x, Double y) {
		this.x = x;
		this.y = y;
		this.coordinates = new Coordinate(x, y);
	}

	/**
	 * Calculate the distance in meters between the current point to the
	 * destination.
	 * 
	 * @param dest
	 * @return distance in meters
	 */
	public double distance(Point dest) {
		if (this.coordinates == null || dest == null || dest.coordinates == null) {
			return 0.0;
		}
		return Math.sqrt((this.coordinates.x - dest.coordinates.x) * (this.coordinates.x - dest.coordinates.x)
				+ (this.coordinates.y - dest.coordinates.y) * (this.coordinates.y - dest.coordinates.y));
	}

	/**
	 * Calculate the distance in meters between the current point to the
	 * destination.
	 * 
	 * @param dest
	 * @return distance in meters
	 */
	public double distance(Coordinate dest) {
		if (this.coordinates == null || dest == null) {
			return 0.0;
		}
		return Math.sqrt((this.coordinates.x - dest.x) * (this.coordinates.x - dest.x)
				+ (this.coordinates.y - dest.y) * (this.coordinates.y - dest.y));
	}

	/**
	 * @param coordinates
	 * @return
	 */
	public static List<Point> extract(String coordinates) {
		if (Strings.isNullOrEmpty(coordinates)) {
			return null;
		}
		List<Point> points = new ArrayList<Point>();
		String[] coor = coordinates.split("\\]\\,\\[");
		for (int i = 0; i < coor.length; i++) {
			String[] point = coor[i].replace("[", "").replace("]", "").split(",");
			if (point.length == 2) {
				points.add(new Point(Double.parseDouble(point[0]), Double.parseDouble(point[1])));
			}
		}
		return points;
	}

	@Override
	public String toString() {
		return "Point [x=" + x + ", y=" + y + "]";
	}

	private void test() {
		Coordinate coordRad01 = new Coordinate(729664.353, 4373064.801);
		Coordinate coordRad02 = new Coordinate(730021.116, 4373070.406);
		Coordinate coordPoi01 = null;
		Coordinate coordPoi02 = null;
		MathTransform transformRadiusToPoint;
		try {
			CoordinateReferenceSystem radiusCRS = CRS.decode("EPSG:32630", true);
			CoordinateReferenceSystem pointCRS = CRS.decode("EPSG:4326", true);

			DirectPosition dp01 = JTS.toDirectPosition(coordRad01, radiusCRS);
			DirectPosition dp02 = JTS.toDirectPosition(coordRad02, radiusCRS);
			System.out.println("DirectPosition dp01: " + dp01 + " dp02: " + dp02);

			Double distance = JTS.orthodromicDistance(coordRad01, coordRad02, radiusCRS);
			System.out.println("Distance: " + distance + " meters");

			transformRadiusToPoint = CRS.findMathTransform(pointCRS, radiusCRS);
			coordPoi01 = JTS.transform(coordRad01, coordPoi01, transformRadiusToPoint);
			coordPoi02 = JTS.transform(coordRad02, coordPoi02, transformRadiusToPoint);

			distance = JTS.orthodromicDistance(coordPoi01, coordPoi02, pointCRS);
			System.out.println("Distance: " + distance + " meters");
		} catch (NoSuchAuthorityCodeException e) {
			e.printStackTrace();
		} catch (FactoryException e) {
			e.printStackTrace();
		} catch (TransformException e) {
			e.printStackTrace();
		}
	}
}

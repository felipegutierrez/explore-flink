package org.sense.flink.pojo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

public class Point implements Serializable {
	private static final long serialVersionUID = -5344602209568822697L;
	private Double x;
	private Double y;
	private String csr;

	public Point(Double x, Double y) {
		this(x, y, null);
	}

	public Point(Double x, Double y, String csr) {
		this.x = x;
		this.y = y;
		this.csr = csr;
	}

	public Point(String coordinatesXY, String csr) {
		String[] xy = coordinatesXY.replace("[", "").replace("]", "").split(",");
		if (xy.length == 2) {
			this.x = Double.parseDouble(xy[0]);
			this.y = Double.parseDouble(xy[1]);
		}
		this.csr = csr;
	}

	/**
	 * Calculate the distance in meters between the current point to the
	 * destination.
	 * 
	 * @param dest
	 * @return distance in meters
	 */
	public double euclideanDistance(Point dest) {
		if (this.x == null || this.y == null || dest == null || dest.x == null || dest.y == null) {
			return 0.0;
		}
		return Math.sqrt((this.x - dest.x) * (this.x - dest.x) + (this.y - dest.y) * (this.y - dest.y));
	}

	/**
	 * @param coordinates
	 * @return
	 */
	public static List<Point> extract(String coordinates, String csr) {
		if (Strings.isNullOrEmpty(coordinates)) {
			return null;
		}
		List<Point> points = new ArrayList<Point>();
		String[] coor = coordinates.split("\\]\\,\\[");
		for (int i = 0; i < coor.length; i++) {
			String[] point = coor[i].replace("[", "").replace("]", "").split(",");
			if (point.length == 2) {
				points.add(new Point(Double.parseDouble(point[0]), Double.parseDouble(point[1]), csr));
			}
		}
		return points;
	}

	@Override
	public String toString() {
		return "Point [x=" + x + ", y=" + y + "]";
	}
}

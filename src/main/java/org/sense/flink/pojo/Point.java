package org.sense.flink.pojo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

public class Point implements Serializable {
	private static final long serialVersionUID = -5344602209568822697L;
	private Double latitude;
	private Double longitude;

	public Point(Double latitude, Double longitude) {
		this.latitude = latitude;
		this.longitude = longitude;
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
		return "Point [latitude=" + latitude + ", longitude=" + longitude + "]";
	}
}

package org.sense.flink.examples.stream.udf.impl;

import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaTraffic;

/**
 * This function finds every coordinate which is close to the point given as
 * parameter within a distance of d and add synthetic data to the stream.
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaTrafficJamSyntheticData implements FlatMapFunction<ValenciaTraffic, ValenciaTraffic> {
	private static final long serialVersionUID = 9217473941578344068L;
	private Point point;
	private double distance;

	/**
	 * Constructor with default values
	 */
	public ValenciaTrafficJamSyntheticData() {
		this(new Point(725737.858, 4370806.847), 100.0);
	}

	public ValenciaTrafficJamSyntheticData(Point point, double distance) {
		this.point = point;
		this.distance = distance;
	}

	@Override
	public void flatMap(ValenciaTraffic value, Collector<ValenciaTraffic> out) throws Exception {
		List<Point> coordinates = value.getCoordinates();
		for (Point p : coordinates) {
			double d = p.euclideanDistance(this.point);
			if (d <= distance) {
				// min = 1 , max = 3, range = (max - min)
				int min = 1, max = 3;
				value.setStatus(new Random().nextInt((max - min) + 1) + min);
				// System.out.println("CHANGED value ValenciaTraffic");
				out.collect(value);
				return;
			}
		}
		// System.out.println("KEEP value as it was received");
		out.collect(value);
	}
}

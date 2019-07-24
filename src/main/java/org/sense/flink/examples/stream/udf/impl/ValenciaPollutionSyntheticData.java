package org.sense.flink.examples.stream.udf.impl;

import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.pojo.AirPollution;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaPollution;

/**
 * This function finds every coordinate which is close to the point given as
 * parameter within a distance of d and add synthetic data to the stream.
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaPollutionSyntheticData implements FlatMapFunction<ValenciaPollution, ValenciaPollution> {
	private static final long serialVersionUID = 7898423692956441572L;
	private Point point;
	private double distance;

	/**
	 * Constructor with default values
	 */
	public ValenciaPollutionSyntheticData() {
		this(new Point(724513.232, 4373018.548), 1500.0);
	}

	public ValenciaPollutionSyntheticData(Point point, double distance) {
		this.point = point;
		this.distance = distance;
	}

	@Override
	public void flatMap(ValenciaPollution value, Collector<ValenciaPollution> out) throws Exception {
		List<Point> coordinates = value.getCoordinates();
		for (Point p : coordinates) {
			double d = p.euclideanDistance(this.point);
			if (d <= distance) {
				value.setParameters(
						new AirPollution(20.0, 20.0, 60.0, 45.0, 40.0, 70.0, 70.0, 85.0, 70.0, 23.0, 13.0, 80.0));
				System.out.println("CHANGED value");
				out.collect(value);
				return;
			}
		}
		System.out.println("KEEP value as it was received");
		out.collect(value);
	}
}

package org.sense.flink.examples.stream.udf.impl;

import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.pojo.AirPollution;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.util.CRSCoordinateTransformer;
import org.sense.flink.util.ValenciaItemType;

/**
 * This function finds every coordinate which is close to the point given as
 * parameter within a distance of d and add synthetic data to the stream.
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaItemSyntheticData implements FlatMapFunction<ValenciaItem, ValenciaItem> {
	private static final long serialVersionUID = 6715124569348951675L;
	private final AirPollution veryBadAir = new AirPollution(20.0, 20.0, 60.0, 45.0, 40.0, 70.0, 70.0, 85.0, 70.0, 23.0,
			13.0, 80.0);

	private ValenciaItemType valenciaItemType;
	private Point point;
	private double distance;
	private Long districtId;

	/**
	 * Constructor with default values
	 */
	public ValenciaItemSyntheticData(ValenciaItemType valenciaItemType) {
		this(valenciaItemType, new Point(725737.858, 4370806.847), 100.0, null);
	}

	public ValenciaItemSyntheticData(ValenciaItemType valenciaItemType, Point point, double distance) {
		this(valenciaItemType, point, distance, null);
	}

	public ValenciaItemSyntheticData(ValenciaItemType valenciaItemType, Point point, double distance, Long districtId) {
		this.valenciaItemType = valenciaItemType;
		this.point = point;
		this.distance = distance;
		this.districtId = districtId;
	}

	@Override
	public void flatMap(ValenciaItem value, Collector<ValenciaItem> out) throws Exception {
		// traffic
		List<Point> coordinates = value.getCoordinates();
		for (Point p : coordinates) {
			double d = p.euclideanDistance(this.point);
			if (d <= distance) {
				if (valenciaItemType == ValenciaItemType.TRAFFIC_JAM) {
					// min = 1 , max = 3, range = (max - min)
					int min = 1, max = 3;
					value.setValue(new Random().nextInt((max - min) + 1) + min);
					// System.out.println("CHANGED value ValenciaItemA");
					out.collect(value);
					return;
				} else if (valenciaItemType == ValenciaItemType.AIR_POLLUTION) {
					value.setValue(veryBadAir);
					// System.out.println("CHANGED value ValenciaPollution");
					out.collect(value);
					return;
				} else if (valenciaItemType == ValenciaItemType.NOISE) {
					// throw new Exception("ValenciaItemType NOISE is not implemented!");
				} else {
					throw new Exception("ValenciaItemType is NULL!");
				}
			} else {
				if (valenciaItemType == ValenciaItemType.TRAFFIC_JAM) {
				} else if (valenciaItemType == ValenciaItemType.AIR_POLLUTION) {
					if (this.districtId != null && value.getId().longValue() <= 5) {
						ValenciaItem anotherValue = (ValenciaItem) value.clone();
						anotherValue.setId(this.districtId);
						anotherValue.clearCoordinates();
						anotherValue.addCoordinates(
								new Point(726777.707, 4369824.436, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE));
						anotherValue.setValue(veryBadAir);
						anotherValue.setDistrict("Quatre Carreres");
						out.collect(anotherValue);
						// System.out.println("CHANGED value ValenciaPollution");
					}
				} else if (valenciaItemType == ValenciaItemType.NOISE) {
				} else {
					throw new Exception("ValenciaItemType is NULL!");
				}
			}
		}
		out.collect(value);
	}
}

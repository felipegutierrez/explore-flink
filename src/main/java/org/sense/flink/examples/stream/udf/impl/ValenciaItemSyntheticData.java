package org.sense.flink.examples.stream.udf.impl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.sense.flink.pojo.AirPollution;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.util.CRSCoordinateTransformer;
import org.sense.flink.util.ValenciaItemType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This function finds every coordinate which is close to the point given as
 * parameter within a distance of d and add synthetic data to the stream.
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaItemSyntheticData extends RichFlatMapFunction<ValenciaItem, ValenciaItem> {
	private static final Logger logger = LoggerFactory.getLogger(ValenciaItemSyntheticData.class);
	private static final long serialVersionUID = 6715124569348951675L;
	private final AirPollution veryBadAir = new AirPollution(20.0, 20.0, 60.0, 45.0, 40.0, 70.0, 70.0, 85.0, 70.0, 23.0,
			13.0, 80.0);

	private ValenciaItemType valenciaItemType;
	private Point point;
	private double distance;
	private Long districtId;
	private String districtName;
	private long startTime;
	private boolean flag;

	/**
	 * Constructor with default values
	 * 
	 * @throws Exception
	 */
	public ValenciaItemSyntheticData(ValenciaItemType valenciaItemType) throws Exception {
		this(valenciaItemType, new Point(725737.858, 4370806.847, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE), 100.0,
				Tuple3.of(3L, 9L, "Extramurs"));
	}

	public ValenciaItemSyntheticData(ValenciaItemType valenciaItemType, Point point, double distance,
			Tuple3<Long, Long, String> adminLevel) throws Exception {
		this.valenciaItemType = valenciaItemType;
		this.point = point;
		this.distance = distance;
		this.districtId = adminLevel.f0;
		this.districtName = adminLevel.f2;
		this.flag = false;
		this.startTime = Calendar.getInstance().getTimeInMillis();
	}

	@Override
	public void flatMap(ValenciaItem value, Collector<ValenciaItem> out) throws Exception {

		long before = Calendar.getInstance().getTimeInMillis() - Time.minutes(5).toMilliseconds();
		if (before >= startTime) {
			startTime = Calendar.getInstance().getTimeInMillis();
			flag = (flag ? false : true);
			String msg = "Changed synthetic data publisher";
			System.out.println(msg);
			logger.info(msg);
		}
		if (flag) {
			for (ValenciaItem item : generateValenciaItem(value, 500)) {
				out.collect(item);
			}
		} else {
			ValenciaItem anotherValenciaItem = changeValenciaItem(value);
			if (anotherValenciaItem != null) {
				out.collect(anotherValenciaItem);
			}
		}
		out.collect(value);
	}

	private List<ValenciaItem> generateValenciaItem(ValenciaItem value, int countMax) throws Exception {
		List<ValenciaItem> list = new ArrayList<ValenciaItem>();
		int count = 0;
		if (valenciaItemType == ValenciaItemType.TRAFFIC_JAM) {
			// min = 1 , max = 3, range = (max - min)
			int min = 1, max = 3;
			while (count < countMax) {
				ValenciaItem item = (ValenciaItem) value.clone();
				item.clearCoordinates();
				item.addCoordinates(point);
				item.setId(districtId);
				item.setDistrict(districtName);
				item.setValue(new Random().nextInt((max - min) + 1) + min);
				list.add(item);
				count++;
			}
		} else if (valenciaItemType == ValenciaItemType.AIR_POLLUTION) {
			if (this.districtId != null && value.getId().longValue() <= 7) {
				while (count < countMax) {
					ValenciaItem item = (ValenciaItem) value.clone();
					item.clearCoordinates();
					item.addCoordinates(point);
					item.setId(districtId);
					item.setDistrict(districtName);
					item.setValue(veryBadAir);
					list.add(item);
					count++;
				}
			}
		} else if (valenciaItemType == ValenciaItemType.NOISE) {
			// throw new Exception("ValenciaItemType NOISE is not implemented!");
		} else {
			throw new Exception("ValenciaItemType is NULL!");
		}
		return list;
	}

	private ValenciaItem changeValenciaItem(ValenciaItem value) throws Exception {
		ValenciaItem anotherValue = (ValenciaItem) value.clone();
		if (valenciaItemType == ValenciaItemType.TRAFFIC_JAM) {
			// min = 1 , max = 3, range = (max - min)
			int min = 1, max = 3;
			List<Point> coordinates = anotherValue.getCoordinates();
			for (Point p : coordinates) {
				double d = p.euclideanDistance(point);
				if (d <= distance) {
					anotherValue.clearCoordinates();
					anotherValue.addCoordinates(point);
					anotherValue.setId(districtId);
					anotherValue.setDistrict(districtName);
					anotherValue.setValue(new Random().nextInt((max - min) + 1) + min);
					break;
				}
			}
		} else if (valenciaItemType == ValenciaItemType.AIR_POLLUTION) {
			if (districtId != null && anotherValue.getId().longValue() <= 7) {
				anotherValue.clearCoordinates();
				anotherValue.addCoordinates(point);
				anotherValue.setId(districtId);
				anotherValue.setDistrict(districtName);
				anotherValue.setValue(veryBadAir);
			}
		} else if (valenciaItemType == ValenciaItemType.NOISE) {
			// throw new Exception("ValenciaItemType NOISE is not implemented!");
		} else {
			throw new Exception("ValenciaItemType is NULL!");
		}
		return anotherValue;
	}
}

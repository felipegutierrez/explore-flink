package org.sense.flink.examples.stream.udf.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
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
	private List<Tuple4<Point, Long, Long, String>> coordinates;
	private long startTime;
	private boolean flag;

	/**
	 * Constructor with default values
	 * 
	 * @throws Exception
	 */
	public ValenciaItemSyntheticData(ValenciaItemType valenciaItemType) throws Exception {
		// @formatter:off
		this(valenciaItemType,
			new ArrayList<Tuple4<Point, Long, Long, String>>(Arrays.asList(
						Tuple4.of(new Point(725140.37, 4371855.492, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE), 3L, 9L, "Extramurs"), 
						Tuple4.of(new Point(726777.707, 4369824.436, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE), 10L, 9L, "Quatre Carreres")
						))
			);
		// @formatter:on
	}

	public ValenciaItemSyntheticData(ValenciaItemType valenciaItemType,
			List<Tuple4<Point, Long, Long, String>> coordinates) throws Exception {
		this.valenciaItemType = valenciaItemType;
		this.coordinates = coordinates;
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

				// get a random point from coordinates
				Tuple4<Point, Long, Long, String> point = getRandomPointFromList(coordinates);
				item.addCoordinates(point.f0);
				item.setId(point.f1);
				item.setDistrict(point.f3);

				item.setValue(new Random().nextInt((max - min) + 1) + min);

				list.add(item);
				count++;
			}
		} else if (valenciaItemType == ValenciaItemType.AIR_POLLUTION) {
			while (count < countMax) {
				ValenciaItem item = (ValenciaItem) value.clone();
				item.clearCoordinates();

				// get a random point from coordinates
				Tuple4<Point, Long, Long, String> point = getRandomPointFromList(coordinates);
				item.addCoordinates(point.f0);
				item.setId(point.f1);
				item.setDistrict(point.f3);

				item.setValue(veryBadAir);

				list.add(item);
				count++;
			}
		} else if (valenciaItemType == ValenciaItemType.NOISE) {
			// throw new Exception("ValenciaItemType NOISE is not implemented!");
		} else {
			throw new Exception("ValenciaItemType is NULL!");
		}
		return list;
	}

	private Tuple4<Point, Long, Long, String> getRandomPointFromList(List<Tuple4<Point, Long, Long, String>> list) {
		int index = ThreadLocalRandom.current().nextInt(list.size());
		System.out.println("\nIndex :" + index);
		return list.get(index);
	}
}

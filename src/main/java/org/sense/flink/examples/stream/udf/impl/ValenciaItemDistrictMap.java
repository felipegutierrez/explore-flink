package org.sense.flink.examples.stream.udf.impl;

import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.util.SimpleGeographicalPolygons;

public class ValenciaItemDistrictMap extends RichMapFunction<ValenciaItem, ValenciaItem> {
	private static final long serialVersionUID = 624354384779615610L;
	private SimpleGeographicalPolygons sgp;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		sgp = new SimpleGeographicalPolygons();
	}

	@Override
	public ValenciaItem map(ValenciaItem value) throws Exception {
		List<Point> coordinates = value.getCoordinates();

		Tuple3<Long, Long, String> adminLevel = sgp.getAdminLevel(coordinates.get(0));
		value.setId(adminLevel.f0);
		value.setAdminLevel(adminLevel.f1);
		value.setDistrict(adminLevel.f2);
		// if (ValenciaItemType.TRAFFIC_JAM == value.getType()) {
		// System.out.println("traffic[" + adminLevel.f0 + "]");
		// } else if (ValenciaItemType.AIR_POLLUTION == value.getType()) {
		// System.out.println("pollution[" + adminLevel.f0 + "]");
		// }
		return value;
	}
}

package org.sense.flink.examples.stream.udf.impl;

import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaTraffic;
import org.sense.flink.util.SimpleGeographicalPolygons;

public class AdminLevelMapFunction extends RichMapFunction<ValenciaTraffic, ValenciaTraffic> {
	private static final long serialVersionUID = 2731115309791316318L;
	private SimpleGeographicalPolygons sgp;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		sgp = new SimpleGeographicalPolygons();
	}

	@Override
	public ValenciaTraffic map(ValenciaTraffic value) throws Exception {
		List<Point> coordinates = value.getCoordinates();

		Tuple3<Long, Long, String> adminLevel = sgp.getAdminLevel(coordinates.get(0));
		value.setId(adminLevel.f0);
		value.setAdminLevel(adminLevel.f1);
		value.setDistrict(adminLevel.f2);
		return value;
	}
}

package org.sense.flink.examples.stream.udf.impl;

import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.util.CRSCoordinateTransformer;
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

		boolean flag = true;
		int i = 0;
		while (flag) {
			Tuple3<Long, Long, String> adminLevel = sgp.getAdminLevel(coordinates.get(i));
			if (adminLevel.f0 != null && adminLevel.f1 != null) {
				value.setId(adminLevel.f0);
				value.setAdminLevel(adminLevel.f1);
				value.setDistrict(adminLevel.f2);
				flag = false;
			} else {
				i++;
			}
		}
		if (flag) {
			// if we did not find a district with the given coordinate we assume the
			// district 16
			value.clearCoordinates();
			value.addCoordinates(new Point(724328.279007, 4374887.874634, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE));
			value.setId(16L);
			value.setAdminLevel(9L);
			value.setDistrict("Benicalap");
		}
		return value;
	}
}

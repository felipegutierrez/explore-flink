package org.sense.flink.examples.stream.udf.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.pojo.ValenciaItemEnriched;
import org.sense.flink.util.SimpleGeographicalPolygons;

public class ValenciaIntensiveCpuDistancesMap extends RichMapFunction<ValenciaItem, ValenciaItemEnriched> {
	private static final long serialVersionUID = -3613815938544784076L;
	private SimpleGeographicalPolygons sgp;

	@Override
	public void open(Configuration parameters) throws Exception {
		this.sgp = new SimpleGeographicalPolygons();
	}

	@Override
	public ValenciaItemEnriched map(ValenciaItem value) throws Exception {
		ValenciaItemEnriched valenciaItemEnriched = new ValenciaItemEnriched(value.getId(), value.getAdminLevel(),
				value.getDistrict(), value.getUpdate(), value.getCoordinates(), value.getValue());

		List<Point> points = valenciaItemEnriched.getCoordinates();
		List<Point> points02 = new ArrayList<Point>(points);

		String allDistances = null;
		for (Point point : points) {
			Tuple3<Long, Long, String> adminLevel = sgp.getAdminLevel(point);
			if (adminLevel.f0 == null || adminLevel.f1 == null) {
				adminLevel.f0 = 16L;
				adminLevel.f1 = 9L;
				adminLevel.f2 = "Benicalap";
			}
			for (Point point02 : points02) {
				Tuple3<Long, Long, String> adminLevel02 = sgp.getAdminLevel(point02);
				if (adminLevel02.f0 == null || adminLevel02.f1 == null) {
					adminLevel02.f0 = 16L;
					adminLevel02.f1 = 9L;
					adminLevel02.f2 = "Benicalap";
				}
				Point pointDerived = sgp.calculateDerivedPosition(point, 10, 10);
				Point pointDerived02 = sgp.calculateDerivedPosition(point02, 10, 10);
				double distance = point.euclideanDistance(point02);
				String msg = "districts[" + adminLevel.f0 + ", " + adminLevel02.f0 + ", " + distance + " meters]";
				if (Strings.isNullOrEmpty(allDistances)) {
					allDistances = "[" + msg;
				} else {
					allDistances += " ;" + msg;
				}
				msg += " " + pointDerived02.toString() + " " + pointDerived.toString();
			}
		}
		allDistances += "]";
		valenciaItemEnriched.setDistances(allDistances);
		return valenciaItemEnriched;
	}
}

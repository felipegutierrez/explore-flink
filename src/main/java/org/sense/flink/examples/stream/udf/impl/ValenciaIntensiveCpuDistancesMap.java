package org.sense.flink.examples.stream.udf.impl;

import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.util.SimpleGeographicalPolygons;

public class ValenciaIntensiveCpuDistancesMap
		extends RichMapFunction<Tuple2<ValenciaItem, ValenciaItem>, Tuple3<ValenciaItem, ValenciaItem, String>> {
	private static final long serialVersionUID = -3613815938544784076L;
	private SimpleGeographicalPolygons sgp;

	@Override
	public void open(Configuration parameters) throws Exception {
		this.sgp = new SimpleGeographicalPolygons();
	}

	@Override
	public Tuple3<ValenciaItem, ValenciaItem, String> map(Tuple2<ValenciaItem, ValenciaItem> value) throws Exception {
		ValenciaItem traffic = value.f0;
		ValenciaItem pollution = value.f1;

		List<Point> trafficPoints = traffic.getCoordinates();
		List<Point> pollutionPoints = pollution.getCoordinates();

		String allDistances = null;
		for (Point trafficPoint : trafficPoints) {
			Tuple3<Long, Long, String> adminLevelTraffic = sgp.getAdminLevel(trafficPoint);
			if (adminLevelTraffic.f0 == null || adminLevelTraffic.f1 == null) {
				adminLevelTraffic.f0 = 16L;
				adminLevelTraffic.f1 = 9L;
				adminLevelTraffic.f2 = "Benicalap";
			}
			for (Point pollutionPoint : pollutionPoints) {
				Tuple3<Long, Long, String> adminLevelPollution = sgp.getAdminLevel(pollutionPoint);
				if (adminLevelPollution.f0 == null || adminLevelPollution.f1 == null) {
					adminLevelPollution.f0 = 16L;
					adminLevelPollution.f1 = 9L;
					adminLevelPollution.f2 = "Benicalap";
				}
				Point pollutionDerived = sgp.calculateDerivedPosition(pollutionPoint, 10, 10);
				Point trafficDerived = sgp.calculateDerivedPosition(trafficPoint, 10, 10);

				double distance = trafficPoint.euclideanDistance(pollutionPoint);
				String msg = "districts[" + adminLevelTraffic.f0 + ", " + adminLevelPollution.f0 + ", " + distance
						+ " meters]";
				if (Strings.isNullOrEmpty(allDistances)) {
					allDistances = "[" + msg;
				} else {
					allDistances += " ;" + msg;
				}
				msg += " " + pollutionDerived.toString() + " " + trafficDerived.toString();
			}
		}
		allDistances += "]";
		return Tuple3.of(traffic, pollution, allDistances);
	}
}

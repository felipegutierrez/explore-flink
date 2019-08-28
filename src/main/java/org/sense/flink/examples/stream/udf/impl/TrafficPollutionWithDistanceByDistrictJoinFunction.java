package org.sense.flink.examples.stream.udf.impl;

import java.util.List;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;

public class TrafficPollutionWithDistanceByDistrictJoinFunction
		implements JoinFunction<ValenciaItem, ValenciaItem, Tuple3<ValenciaItem, ValenciaItem, String>> {
	private static final long serialVersionUID = -8504552660558428217L;

	@Override
	public Tuple3<ValenciaItem, ValenciaItem, String> join(ValenciaItem traffic, ValenciaItem pollution)
			throws Exception {
		ValenciaItem trafficClone = (ValenciaItem) traffic.clone();
		ValenciaItem pollutionClone = (ValenciaItem) pollution.clone();

		List<Point> trafficPoints = trafficClone.getCoordinates();
		List<Point> pollutionPoints = pollutionClone.getCoordinates();

		String distances = "Distances[";
		for (Point trafficPoint : trafficPoints) {
			for (Point pollutionPoint : pollutionPoints) {
				distances += trafficPoint.euclideanDistance(pollutionPoint) + " , ";
			}
		}
		distances += "] meters.";

		trafficClone.clearCoordinates();
		pollutionClone.clearCoordinates();
		return Tuple3.of(trafficClone, pollutionClone, distances);
	}
}

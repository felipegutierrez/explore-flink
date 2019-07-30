package org.sense.flink.examples.stream.udf.impl;

import java.util.List;

import org.apache.flink.api.common.functions.JoinFunction;
import org.sense.flink.pojo.AirPollution;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaPollution;
import org.sense.flink.pojo.ValenciaTraffic;
import org.sense.flink.util.TrafficStatus;

public class TrafficPollutionByDistrictJoinFunction
		implements JoinFunction<ValenciaTraffic, ValenciaPollution, String> {
	private static final long serialVersionUID = -5177819691797616298L;

	@Override
	public String join(ValenciaTraffic traffic, ValenciaPollution pollution) throws Exception {
		Long id = traffic.getId();
		Long adminLevel = traffic.getAdminLevel();
		String district = traffic.getDistrict();
		Integer trafficStatus = traffic.getStatus();
		List<Point> trafficPoints = traffic.getCoordinates();
		AirPollution pollutionParam = pollution.getParameters();
		List<Point> pollutionPoints = pollution.getCoordinates();

		String districtDesc = id + " " + district + " " + adminLevel;
		String trafficStatusDesc = TrafficStatus.getStatus(trafficStatus);
		return districtDesc + " Traffic[" + trafficStatusDesc + "] " + pollutionParam;
	}
}

package org.sense.flink.examples.stream.udf.impl;

import java.util.List;

import org.apache.flink.api.common.functions.JoinFunction;
import org.sense.flink.pojo.AirPollution;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.util.TrafficStatus;

public class TrafficPollutionByDistrictJoinFunction implements JoinFunction<ValenciaItem, ValenciaItem, String> {
	private static final long serialVersionUID = 8610821472997589795L;

	@Override
	public String join(ValenciaItem traffic, ValenciaItem pollution) throws Exception {
		Long id = traffic.getId();
		Long adminLevel = traffic.getAdminLevel();
		String district = traffic.getDistrict();
		Integer trafficStatus = (Integer) traffic.getValue();
		List<Point> trafficPoints = traffic.getCoordinates();
		AirPollution pollutionParam = (AirPollution) pollution.getValue();
		List<Point> pollutionPoints = pollution.getCoordinates();

		String districtDesc = id + " " + district + " " + adminLevel;
		String trafficStatusDesc = TrafficStatus.getStatus(trafficStatus);
		return districtDesc + " Traffic[" + trafficStatusDesc + "] " + pollutionParam;
	}
}

package org.sense.flink.examples.stream.udf.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.sense.flink.pojo.AirPollution;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.pojo.ValenciaItemEnriched;
import org.sense.flink.util.SimpleGeographicalPolygons;
import org.sense.flink.util.ValenciaItemType;

public class ValenciaIntensiveCpuDistancesMap
		extends RichMapFunction<Tuple2<ValenciaItem, ValenciaItem>, ValenciaItemEnriched> {
	private static final long serialVersionUID = -3613815938544784076L;
	private SimpleGeographicalPolygons sgp;

	@Override
	public void open(Configuration parameters) throws Exception {
		this.sgp = new SimpleGeographicalPolygons();
	}

	@Override
	public ValenciaItemEnriched map(Tuple2<ValenciaItem, ValenciaItem> value) throws Exception {

		ValenciaItem valenciaItem00 = value.f0;
		ValenciaItem valenciaItem01 = value.f1;
		List<Point> coordinates00 = valenciaItem00.getCoordinates();
		List<Point> coordinates01 = valenciaItem01.getCoordinates();
		List<Point> newCoordinates = new ArrayList<Point>();
		newCoordinates.addAll(coordinates00);
		newCoordinates.addAll(coordinates01);
		String newValue = "";

		if (valenciaItem00.getType() == ValenciaItemType.TRAFFIC_JAM) {
			if (valenciaItem00.getValue() instanceof AirPollution) {
				System.out.println("error");
			}
			newValue = ((Integer) valenciaItem00.getValue()).toString();
		} else if (valenciaItem00.getType() == ValenciaItemType.AIR_POLLUTION) {
			newValue = ((AirPollution) valenciaItem00.getValue()).toString();
		} else if (valenciaItem00.getType() == ValenciaItemType.NOISE) {
		} else {
			throw new Exception("ValenciaItemType is NULL!");
		}
		if (valenciaItem01.getType() == ValenciaItemType.TRAFFIC_JAM) {
			newValue += " - " + ((Integer) valenciaItem01.getValue()).toString();
		} else if (valenciaItem01.getType() == ValenciaItemType.AIR_POLLUTION) {
			newValue += " - " + ((AirPollution) valenciaItem01.getValue()).toString();
		} else if (valenciaItem01.getType() == ValenciaItemType.NOISE) {
		} else {
			throw new Exception("ValenciaItemType is NULL!");
		}
		ValenciaItemEnriched valenciaItemEnriched = new ValenciaItemEnriched(valenciaItem00.getId(),
				valenciaItem00.getAdminLevel(), valenciaItem00.getDistrict(), valenciaItem00.getUpdate(),
				newCoordinates, newValue);

		String allDistances = null;
		for (Point point00 : coordinates00) {
			Tuple3<Long, Long, String> adminLevel00 = sgp.getAdminLevel(point00);
			if (adminLevel00.f0 == null || adminLevel00.f1 == null) {
				adminLevel00.f0 = 16L;
				adminLevel00.f1 = 9L;
				adminLevel00.f2 = "Benicalap";
			}
			for (Point point01 : coordinates01) {
				Tuple3<Long, Long, String> adminLevel01 = sgp.getAdminLevel(point01);
				if (adminLevel01.f0 == null || adminLevel01.f1 == null) {
					adminLevel01.f0 = 16L;
					adminLevel01.f1 = 9L;
					adminLevel01.f2 = "Benicalap";
				}
				Point pointDerived = sgp.calculateDerivedPosition(point00, 10, 10);
				Point pointDerived02 = sgp.calculateDerivedPosition(point01, 10, 10);
				double distance = point00.euclideanDistance(point01);
				String msg = "districts[" + adminLevel00.f0 + ", " + adminLevel01.f0 + ", " + distance + " meters]";
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

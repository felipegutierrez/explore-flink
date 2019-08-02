package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.sense.flink.pojo.AirPollution;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.pojo.ValenciaItemAvg;
import org.sense.flink.pojo.ValenciaPollution;
import org.sense.flink.pojo.ValenciaTraffic;
import org.sense.flink.util.ValenciaItemType;

public class ValenciaDistrictItemTypeAggWindow
		extends RichWindowFunction<Tuple2<Long, ValenciaItem>, Tuple2<Long, ValenciaItem>, Long, TimeWindow> {
	private static final long serialVersionUID = 6512873974443741961L;
	private transient Meter meter;
	private String metricName;

	public ValenciaDistrictItemTypeAggWindow() {
		this.metricName = StationPlatformRichWindowFunction.class.getSimpleName();
	}

	public ValenciaDistrictItemTypeAggWindow(String metricName) {
		this.metricName = metricName;
	}

	@Override
	public void open(Configuration config) throws Exception {
		com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
		this.meter = getRuntimeContext().getMetricGroup().meter(this.metricName + "-meter",
				new DropwizardMeterWrapper(dropwizardMeter));
	}

	@Override
	public void apply(Long key, TimeWindow window, Iterable<Tuple2<Long, ValenciaItem>> input,
			Collector<Tuple2<Long, ValenciaItem>> out) throws Exception {
		this.meter.markEvent();
		Long district = key;
		ValenciaItem valenciaItemAvg = null;
		ValenciaItem valenciaTraffic = null;
		ValenciaItem valenciaPollution = null;
		Long count = 0L;

		// compute the sum of values of all items
		for (Tuple2<Long, ValenciaItem> tuple2 : input) {
			if (valenciaItemAvg == null) {
				valenciaItemAvg = new ValenciaItemAvg(tuple2.f1.getId(), tuple2.f1.getAdminLevel(),
						tuple2.f1.getDistrict(), tuple2.f1.getUpdate(), null, null);
			}

			if (tuple2.f1.getType() == ValenciaItemType.TRAFFIC_JAM) {
				if (valenciaTraffic == null) {
					valenciaTraffic = new ValenciaTraffic(tuple2.f1.getId(), tuple2.f1.getAdminLevel(),
							tuple2.f1.getDistrict(), tuple2.f1.getUpdate(), null, (Integer) tuple2.f1.getValue());
				} else {
					// this is consuming too much memory
					// valenciaItem.addCoordinates(tuple2.f1.getCoordinates());
					valenciaTraffic.addValue(tuple2.f1.getValue());
				}
			} else if (tuple2.f1.getType() == ValenciaItemType.AIR_POLLUTION) {
				if (valenciaPollution == null) {
					valenciaPollution = new ValenciaPollution(tuple2.f1.getId(), tuple2.f1.getAdminLevel(),
							tuple2.f1.getDistrict(), tuple2.f1.getUpdate(), null, (AirPollution) tuple2.f1.getValue());
				} else {
					// this is consuming too much memory
					// valenciaItem.addCoordinates(tuple2.f1.getCoordinates());
					valenciaPollution.addValue(tuple2.f1.getValue());
				}
			} else if (tuple2.f1.getType() == ValenciaItemType.NOISE) {
				throw new Exception("ValenciaItemType NOISE is not implemented!");
			} else {
				throw new Exception("ValenciaItemType is NULL!");
			}
			count++;
		}
		String average = "";
		if (valenciaTraffic != null && valenciaTraffic.getValue() != null) {
			String avgTraffic = "Traffic average [" + ((Integer) valenciaTraffic.getValue()).toString() + "] ";
			average = average + avgTraffic;
		}
		if (valenciaPollution != null && valenciaPollution.getValue() != null) {
			String avgPollution = "Pollution average [" + ((AirPollution) valenciaPollution.getValue()).toString()
					+ "] ";
			average = average + avgPollution;
		}
		valenciaItemAvg.setValue(average);
		out.collect(Tuple2.of(count, valenciaItemAvg));
	}
}

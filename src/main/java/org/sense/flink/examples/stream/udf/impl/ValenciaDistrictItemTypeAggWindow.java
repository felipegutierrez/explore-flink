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
import org.sense.flink.pojo.ValenciaPollution;
import org.sense.flink.pojo.ValenciaTraffic;
import org.sense.flink.util.ValenciaItemType;

public class ValenciaDistrictItemTypeAggWindow extends
		RichWindowFunction<Tuple2<Long, ValenciaItem>, Tuple2<Long, ValenciaItem>, Tuple2<Long, ValenciaItemType>, TimeWindow> {
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
	public void apply(Tuple2<Long, ValenciaItemType> key, TimeWindow window, Iterable<Tuple2<Long, ValenciaItem>> input,
			Collector<Tuple2<Long, ValenciaItem>> out) throws Exception {
		this.meter.markEvent();
		Long district = key.f0;
		ValenciaItem valenciaItem = null;
		Long count = 0L;

		// compute the sum of values of all items
		for (Tuple2<Long, ValenciaItem> tuple2 : input) {
			if (key.f1 == ValenciaItemType.TRAFFIC_JAM) {
				if (valenciaItem == null) {
					valenciaItem = new ValenciaTraffic(tuple2.f1.getId(), tuple2.f1.getAdminLevel(),
							tuple2.f1.getDistrict(), tuple2.f1.getUpdate(), null, (Integer) tuple2.f1.getValue());
				} else {
					// valenciaItem.addCoordinates(tuple2.f1.getCoordinates());
					valenciaItem.addValue(tuple2.f1.getValue());
				}
			} else if (key.f1 == ValenciaItemType.AIR_POLLUTION) {
				if (valenciaItem == null) {
					valenciaItem = new ValenciaPollution(tuple2.f1.getId(), tuple2.f1.getAdminLevel(),
							tuple2.f1.getDistrict(), tuple2.f1.getUpdate(), null, (AirPollution) tuple2.f1.getValue());
				} else {
					// valenciaItem.addCoordinates(tuple2.f1.getCoordinates());
					valenciaItem.addValue(tuple2.f1.getValue());
				}
			} else if (key.f1 == ValenciaItemType.NOISE) {
				throw new Exception("ValenciaItemType NOISE is not implemented!");
			} else {
				throw new Exception("ValenciaItemType is NULL!");
			}
			count++;
		}
		out.collect(Tuple2.of(count, valenciaItem));
	}
}

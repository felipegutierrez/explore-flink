package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.sense.flink.pojo.ValenciaItem;

public class ValenciaItemAggWindow
		extends RichWindowFunction<Tuple2<Long, ValenciaItem>, Tuple2<Long, ValenciaItem>, Long, TimeWindow> {
	private static final long serialVersionUID = 6512873974443741961L;
	private transient Meter meter;
	private String metricName;

	public ValenciaItemAggWindow() {
		this.metricName = StationPlatformRichWindowFunction.class.getSimpleName();
	}

	public ValenciaItemAggWindow(String metricName) {
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
		for (Tuple2<Long, ValenciaItem> tuple2 : input) {
			Long district = tuple2.f0;
			ValenciaItem valenciaItem = tuple2.f1;

			out.collect(Tuple2.of(district, valenciaItem));
		}
	}
}

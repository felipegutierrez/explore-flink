package org.sense.flink.examples.stream;

import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_DISTRICT_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_FILTER;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SINK;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SOURCE;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SYNTHETIC_FLATMAP;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.udf.impl.TrafficPollutionByDistrictJoinFunction;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictSelector;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemFilter;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemSyntheticData;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.source.ValenciaItemConsumer;
import org.sense.flink.util.CRSCoordinateTransformer;
import org.sense.flink.util.ValenciaItemType;

/**
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaDataSkewedJoinExample {

	public static void main(String[] args) throws Exception {
		new ValenciaDataSkewedJoinExample();
	}

	public ValenciaDataSkewedJoinExample() throws Exception {
		disclaimer();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// @formatter:off
		// Static coordinate to create synthetic data
		// Point point = new Point(725140.37, 4371855.492, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE); // id=3, district=Extramurs
		// Tuple3<Long, Long, String> adminLevel = Tuple3.of(3L, 9L, "Extramurs");
		Point point = new Point(726777.707, 4369824.436, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE); // id=10, district=Quatre Carreres
		Tuple3<Long, Long, String> adminLevel = Tuple3.of(10L, 9L, "Quatre Carreres");
		double distance = 1000.0; // distance in meters

		// Sources -> add synthetic data -> filter
		DataStream<ValenciaItem> streamTrafficJam = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, Time.minutes(5).toMilliseconds(), false)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.TRAFFIC_JAM)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				.flatMap(new ValenciaItemSyntheticData(ValenciaItemType.TRAFFIC_JAM, point, distance, adminLevel)).name(METRIC_VALENCIA_SYNTHETIC_FLATMAP)
				.filter(new ValenciaItemFilter(ValenciaItemType.TRAFFIC_JAM)).name(METRIC_VALENCIA_FILTER)
				;

		DataStream<ValenciaItem> streamAirPollution = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, Time.minutes(30).toMilliseconds(), false)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.AIR_POLLUTION)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				.flatMap(new ValenciaItemSyntheticData(ValenciaItemType.AIR_POLLUTION, point, distance, adminLevel)).name(METRIC_VALENCIA_SYNTHETIC_FLATMAP)
				.filter(new ValenciaItemFilter(ValenciaItemType.AIR_POLLUTION)).name(METRIC_VALENCIA_FILTER)
				;

		// Join -> Print
		streamTrafficJam.join(streamAirPollution)
				.where(new ValenciaItemDistrictSelector())
 				.equalTo(new ValenciaItemDistrictSelector())
		 		.window(TumblingEventTimeWindows.of(Time.seconds(20)))
		 		.apply(new TrafficPollutionByDistrictJoinFunction())
		 		.print().name(METRIC_VALENCIA_SINK)
		  		;
		// streamTrafficJam.print();
		// streamAirPollution.print();

		env.execute(ValenciaDataSkewedJoinExample.class.getName());
		// @formatter:on
	}

	private void disclaimer() {
		System.out.println("Disclaimer...");
	}
}

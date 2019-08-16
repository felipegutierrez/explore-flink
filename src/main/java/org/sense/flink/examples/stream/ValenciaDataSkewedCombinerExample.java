package org.sense.flink.examples.stream;

import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_COMBINER;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_DISTRICT_KEY_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_DISTRICT_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SINK;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SOURCE;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_STRING_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_WINDOW;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.operator.impl.MapStreamBundleOperatorDynamic;
import org.sense.flink.examples.stream.trigger.impl.CountBundleTriggerDynamic;
import org.sense.flink.examples.stream.udf.MapBundleFunction;
import org.sense.flink.examples.stream.udf.impl.MapBundleValenciaImpl;
import org.sense.flink.examples.stream.udf.impl.ValenciaDistrictItemTypeAggWindow;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictAsKeyMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemKeySelector;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemToStringMap;
import org.sense.flink.mqtt.MqttStringPublisher;
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
public class ValenciaDataSkewedCombinerExample {

	private final String topic = "topic-valencia-data-skewed";

	public static void main(String[] args) throws Exception {
		new ValenciaDataSkewedCombinerExample("127.0.0.1", "127.0.0.1");
	}

	public ValenciaDataSkewedCombinerExample(String ipAddressSource01, String ipAddressSink) throws Exception {
		disclaimer();
		List<Tuple4<Point, Long, Long, String>> coordinates = syntheticCoordinates();
		boolean offlineData = true;
		boolean collectWithTimestamp = true;
		boolean skewedDataInjection = true;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// @formatter:off
		// Sources -> add synthetic data -> filter
		DataStream<Tuple2<Long , ValenciaItem>> streamTrafficJam = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, Time.seconds(20).toMilliseconds(), collectWithTimestamp, offlineData, skewedDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.TRAFFIC_JAM)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				// .flatMap(new ValenciaItemSyntheticData(ValenciaItemType.TRAFFIC_JAM, coordinates)).name(METRIC_VALENCIA_SYNTHETIC_FLATMAP)
				.map(new ValenciaItemDistrictAsKeyMap()).name(METRIC_VALENCIA_DISTRICT_KEY_MAP)
				;
		DataStream<Tuple2<Long , ValenciaItem>> streamAirPollution = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, Time.seconds(50).toMilliseconds(), collectWithTimestamp, offlineData, skewedDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.AIR_POLLUTION)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				// .flatMap(new ValenciaItemSyntheticData(ValenciaItemType.AIR_POLLUTION, coordinates)).name(METRIC_VALENCIA_SYNTHETIC_FLATMAP)
				.map(new ValenciaItemDistrictAsKeyMap()).name(METRIC_VALENCIA_DISTRICT_KEY_MAP)
				;

		// Combiner
		MapBundleFunction<Long, ValenciaItem, Tuple2<Long, ValenciaItem>, Tuple2<Long, ValenciaItem>> myMapBundleFunction = new MapBundleValenciaImpl();
		// CountBundleTrigger<Tuple2<Long, ValenciaItem>> bundleTrigger = new CountBundleTrigger<Tuple2<Long, ValenciaItem>>(10);
		CountBundleTriggerDynamic<Long, Tuple2<Long, ValenciaItem>> bundleTrigger = new CountBundleTriggerDynamic<Long, Tuple2<Long, ValenciaItem>>();
		KeySelector<Tuple2<Long, ValenciaItem>, Long> keyBundleSelector = (KeySelector<Tuple2<Long, ValenciaItem>, Long>) value -> value.f0;
		TypeInformation<Tuple2<Long, ValenciaItem>> info = TypeInformation.of(new TypeHint<Tuple2<Long, ValenciaItem>>(){});

		// Union -> Combiner -> Average -> Print
		streamTrafficJam.union(streamAirPollution)
				// .transform(METRIC_VALENCIA_COMBINER, info, new MapStreamBundleOperator<>(myMapBundleFunction, bundleTrigger, keyBundleSelector)).name(METRIC_VALENCIA_COMBINER)
				.transform(METRIC_VALENCIA_COMBINER, info, new MapStreamBundleOperatorDynamic<>(myMapBundleFunction, bundleTrigger, keyBundleSelector)).name(METRIC_VALENCIA_COMBINER)
				.keyBy(new ValenciaItemKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
				.apply(new ValenciaDistrictItemTypeAggWindow()).name(METRIC_VALENCIA_WINDOW)
				.map(new ValenciaItemToStringMap()).name(METRIC_VALENCIA_STRING_MAP)
				.addSink(new MqttStringPublisher(ipAddressSink, topic)).name(METRIC_VALENCIA_SINK)
				// .print().name(METRIC_VALENCIA_SINK)
				;
		//streamTrafficJam.print();
		//streamAirPollution.print();

		env.execute(ValenciaDataSkewedCombinerExample.class.getName());
		// @formatter:on
	}

	private List<Tuple4<Point, Long, Long, String>> syntheticCoordinates() {
		// @formatter:off
		// Static coordinate to create synthetic data
		List<Tuple4<Point, Long, Long, String>> coordinates = new ArrayList<Tuple4<Point, Long, Long, String>>();
		// coordinates.add(Tuple4.of(new Point(725685.2117, 4372281.7883, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE), 1L, 9L, "l'Eixample"));
		// coordinates.add(Tuple4.of(new Point(725140.37, 4371855.492, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE), 3L, 9L, "Extramurs"));
		// coordinates.add(Tuple4.of(new Point(722864.4094000002, 4373373.1132, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE), 4L, 9L, "Campanar"));
		// coordinates.add(Tuple4.of(new Point(726236.4035999998, 4373308.101, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE), 5L, 9L, "la Saïdia"));
		// coordinates.add(Tuple4.of(new Point(726423.431815, 4373034.130474, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE), 6L, 9L, "Pla del Real"));
		// coordinates.add(Tuple4.of(new Point(723969.784325, 4372420.897437, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE), 7L, 9L, "Olivereta"));
		// coordinates.add(Tuple4.of(new Point(723623.655515, 4370778.759536, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE), 8L, 9L, "Patraix"));
		// points.add(new Point(724034.3761, 4369995.1312, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE)); // 9,9,Jesús
		coordinates.add(Tuple4.of(new Point(726777.707, 4369824.436, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE), 10L, 9L, "Quatre Carreres"));
		// points.add(new Point(730007.679276, 4369416.885897, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE)); // 11,9,Poblats Maritims
		// points.add(new Point(728630.3849999998, 4370921.0409, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE)); // 12,9,Camins al Grau
		// points.add(new Point(729010.3865999999, 4373390.0215, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE)); // 13,9,Algirós
		// points.add(new Point(727338.396370, 4374107.624796, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE)); // 14,9,Benimaclet 
		// points.add(new Point(726240.844004, 4375087.354217, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE)); // 15,9,Rascanya
		// points.add(new Point(724328.279007, 4374887.874634, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE)); // 16,9,Benicalap
		return coordinates;
		// @formatter:on
	}

	private void disclaimer() {
		System.out.println("Disclaimer...");
	}
}

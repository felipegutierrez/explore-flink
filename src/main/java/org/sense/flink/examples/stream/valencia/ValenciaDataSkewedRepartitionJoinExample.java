package org.sense.flink.examples.stream.valencia;

import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_DISTRICT_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_JOIN;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SINK;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SOURCE;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_STRING_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SYNTHETIC_FLATMAP;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.udf.impl.Valencia2ItemToStringMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictSelector;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemProcessingTimeStdRepartitionJoinCoProcess;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemSyntheticData;
import org.sense.flink.mqtt.MqttStringPublisher;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.source.ValenciaItemConsumer;
import org.sense.flink.util.CRSCoordinateTransformer;
import org.sense.flink.util.ValenciaItemType;

/**
 * 
 * based on
 * 
 * https://github.com/ververica/flink-training-exercises/blob/master/src/main/java/com/dataartisans/flinktraining/exercises/datastream_java/process/ExpiringStateExercise.java
 * https://github.com/ververica/flink-training-exercises/blob/master/src/main/java/com/dataartisans/flinktraining/exercises/datastream_java/process/ProcessingTimeJoinExercise.java
 * https://github.com/ververica/flink-training-exercises/blob/master/src/main/java/com/dataartisans/flinktraining/exercises/datastream_java/process/EventTimeJoinExercise.java
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaDataSkewedRepartitionJoinExample {

	private final String topic = "topic-valencia-join-data-skewed";

	public static void main(String[] args) throws Exception {
		new ValenciaDataSkewedRepartitionJoinExample("127.0.0.1", "127.0.0.1");
	}

	public ValenciaDataSkewedRepartitionJoinExample(String ipAddressSource01, String ipAddressSink) throws Exception {
		disclaimer();
		List<Tuple4<Point, Long, Long, String>> coordinates = syntheticCoordinates();
		boolean offlineData = true;
		boolean collectWithTimestamp = true;
		boolean skewedDataInjection = true;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		// @formatter:off
		// Sources -> add synthetic data -> filter
		DataStream<ValenciaItem> streamTrafficJam = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, Time.seconds(20).toMilliseconds(), collectWithTimestamp, offlineData, skewedDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.TRAFFIC_JAM)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				.flatMap(new ValenciaItemSyntheticData(ValenciaItemType.TRAFFIC_JAM, coordinates)).name(METRIC_VALENCIA_SYNTHETIC_FLATMAP)
				// .filter(new ValenciaItemFilter(ValenciaItemType.TRAFFIC_JAM)).name(METRIC_VALENCIA_FILTER)
				;

		DataStream<ValenciaItem> streamAirPollution = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, Time.seconds(60).toMilliseconds(), collectWithTimestamp, offlineData, skewedDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.AIR_POLLUTION)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				.flatMap(new ValenciaItemSyntheticData(ValenciaItemType.AIR_POLLUTION, coordinates)).name(METRIC_VALENCIA_SYNTHETIC_FLATMAP)
				// .filter(new ValenciaItemFilter(ValenciaItemType.AIR_POLLUTION)).name(METRIC_VALENCIA_FILTER)
				;

		streamTrafficJam
				.keyBy(new ValenciaItemDistrictSelector())
				.connect(streamAirPollution.keyBy(new ValenciaItemDistrictSelector()))
				// .process(new ValenciaItemEventTimeJoinCoProcess())
				.process(new ValenciaItemProcessingTimeStdRepartitionJoinCoProcess(Time.seconds(10).toMilliseconds())).name(METRIC_VALENCIA_JOIN)
				.map(new Valencia2ItemToStringMap()).name(METRIC_VALENCIA_STRING_MAP)
				.addSink(new MqttStringPublisher(ipAddressSink, topic)).name(METRIC_VALENCIA_SINK)
				// .print().name(METRIC_VALENCIA_SINK)
				;

		// streamTrafficJam.print();
		// streamAirPollution.print();

		env.execute(ValenciaDataSkewedRepartitionJoinExample.class.getName());
		// @formatter:on
	}

	private List<Tuple4<Point, Long, Long, String>> syntheticCoordinates() {
		// Static coordinate to create synthetic data
		List<Tuple4<Point, Long, Long, String>> coordinates = new ArrayList<Tuple4<Point, Long, Long, String>>();
		coordinates.add(Tuple4.of(new Point(725140.37, 4371855.492, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830), 3L,
				9L, "Extramurs"));
		coordinates.add(Tuple4.of(new Point(726777.707, 4369824.436, CRSCoordinateTransformer.DEFAULT_CRS_EPSG_25830), 10L,
				9L, "Quatre Carreres"));
		return coordinates;
	}

	private void disclaimer() {
		System.out.println("Disclaimer...");
	}
}

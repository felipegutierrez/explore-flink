package org.sense.flink.examples.stream.valencia;

import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_DISTRICT_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_JOIN;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SINK;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SOURCE;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_STRING_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SYNTHETIC_FLATMAP;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.udf.impl.Valencia2ItemToStringMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictSelector;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemProcessingTimeBroadcastJoinKeyedBroadcastProcess;
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
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/state/broadcast_state.html
 * https://flink.apache.org/2019/06/26/broadcast-state.html
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaDataSkewedBroadcastJoinExample {

	private final String topic = "topic-valencia-join-data-skewed";

	public static void main(String[] args) throws Exception {
		new ValenciaDataSkewedBroadcastJoinExample("127.0.0.1", "127.0.0.1");
	}

	public ValenciaDataSkewedBroadcastJoinExample(String ipAddressSource01, String ipAddressSink) throws Exception {
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
				;

		DataStream<ValenciaItem> streamAirPollution = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, Time.seconds(60).toMilliseconds(), collectWithTimestamp, offlineData, skewedDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.AIR_POLLUTION)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				;

		// Broadcast pollution stream
		MapStateDescriptor<Long, ValenciaItem> bcStateDescriptor = new MapStateDescriptor<Long, ValenciaItem>("PollutionBroadcastState",
				Types.LONG, TypeInformation.of(new TypeHint<ValenciaItem>() {}));
		BroadcastStream<ValenciaItem> bcStreamPollution = streamAirPollution.broadcast(bcStateDescriptor);

		streamTrafficJam
				.keyBy(new ValenciaItemDistrictSelector())
				.connect(bcStreamPollution)
				.process(new ValenciaItemProcessingTimeBroadcastJoinKeyedBroadcastProcess(Time.seconds(10).toMilliseconds())).name(METRIC_VALENCIA_JOIN)
				.map(new Valencia2ItemToStringMap()).name(METRIC_VALENCIA_STRING_MAP)
				.addSink(new MqttStringPublisher(ipAddressSink, topic)).name(METRIC_VALENCIA_SINK)
				;

		env.execute(ValenciaDataSkewedBroadcastJoinExample.class.getName());
		// @formatter:on
	}

	private List<Tuple4<Point, Long, Long, String>> syntheticCoordinates() {
		// Static coordinate to create synthetic data
		List<Tuple4<Point, Long, Long, String>> coordinates = new ArrayList<Tuple4<Point, Long, Long, String>>();
		coordinates.add(Tuple4.of(new Point(725140.37, 4371855.492, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE), 3L,
				9L, "Extramurs"));
		coordinates.add(Tuple4.of(new Point(726777.707, 4369824.436, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE), 10L,
				9L, "Quatre Carreres"));
		return coordinates;
	}

	private void disclaimer() {
		System.out.println("Disclaimer...");
	}
}

package org.sense.flink.examples.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.udf.impl.ValenciaDistrictAndItemTypeKeySelector;
import org.sense.flink.examples.stream.udf.impl.ValenciaDistrictItemTypeAggWindow;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictAsKeyMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemSyntheticData;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemToStringMap;
import org.sense.flink.mqtt.MqttStringPublisher;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.source.ValenciaItemConsumer;
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

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// @formatter:off
		// Static coordinate to create synthetic data
		// Point point = new Point(725140.37, 4371855.492); // id=3, district=Extramurs
		Point point = new Point(726777.707, 4369824.436); // id=10, district=Quatre Carreres
		double distance = 1000.0; // distance in meters
		long districtId = 10;

		// Sources -> add synthetic data -> filter
		DataStream<Tuple2<Long , ValenciaItem>> streamTrafficJam = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, Time.minutes(5).toMilliseconds())).name(ValenciaItemConsumer.class.getName())
				.map(new ValenciaItemDistrictMap()).name(ValenciaItemDistrictMap.class.getName())
				.flatMap(new ValenciaItemSyntheticData(ValenciaItemType.TRAFFIC_JAM, point, distance)).name(ValenciaItemSyntheticData.class.getName())
				.map(new ValenciaItemDistrictAsKeyMap()).name(ValenciaItemDistrictAsKeyMap.class.getName())
				;
		DataStream<Tuple2<Long , ValenciaItem>> streamAirPollution = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, Time.minutes(30).toMilliseconds())).name(ValenciaItemConsumer.class.getName())
				.map(new ValenciaItemDistrictMap()).name(ValenciaItemDistrictMap.class.getName())
				.flatMap(new ValenciaItemSyntheticData(ValenciaItemType.AIR_POLLUTION, point, distance, districtId)).name(ValenciaItemSyntheticData.class.getName())
				.map(new ValenciaItemDistrictAsKeyMap()).name(ValenciaItemDistrictAsKeyMap.class.getName())
				;

		// Combine -> Print
		streamTrafficJam.union(streamAirPollution)
				.keyBy(new ValenciaDistrictAndItemTypeKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
				.apply(new ValenciaDistrictItemTypeAggWindow()).name(ValenciaDistrictItemTypeAggWindow.class.getName())
				.map(new ValenciaItemToStringMap()).name(ValenciaItemToStringMap.class.getName())
				.addSink(new MqttStringPublisher(ipAddressSink, topic)).name(MqttStringPublisher.class.getName())
				// .print()
				;

		env.execute(ValenciaDataSkewedCombinerExample.class.getName());
		// @formatter:on
	}

	private void disclaimer() {
		System.out.println("Disclaimer...");
	}
}

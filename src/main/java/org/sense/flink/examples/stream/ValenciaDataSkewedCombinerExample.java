package org.sense.flink.examples.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictAsKeyMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemSyntheticData;
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

	public static void main(String[] args) throws Exception {
		new ValenciaDataSkewedCombinerExample();
	}

	public ValenciaDataSkewedCombinerExample() throws Exception {
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
				.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC, Time.minutes(5))).name(ValenciaItemConsumer.class.getName())
				.map(new ValenciaItemDistrictMap()).name(ValenciaItemDistrictMap.class.getName())
				.flatMap(new ValenciaItemSyntheticData(ValenciaItemType.TRAFFIC, point, distance)).name(ValenciaItemSyntheticData.class.getName())
				.map(new ValenciaItemDistrictAsKeyMap()).name(ValenciaItemDistrictAsKeyMap.class.getName())
				;
		DataStream<Tuple2<Long , ValenciaItem>> streamAirPollution = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, Time.minutes(30))).name(ValenciaItemConsumer.class.getName())
				.map(new ValenciaItemDistrictMap()).name(ValenciaItemDistrictMap.class.getName())
				.flatMap(new ValenciaItemSyntheticData(ValenciaItemType.AIR_POLLUTION, point, distance, districtId)).name(ValenciaItemSyntheticData.class.getName())
				.map(new ValenciaItemDistrictAsKeyMap()).name(ValenciaItemDistrictAsKeyMap.class.getName())
				;

		// Combine -> Print
		// streamTrafficJam.union(streamAirPollution)
				// .print()
		;
		streamTrafficJam.print();
		streamAirPollution.print();

		env.execute(ValenciaDataSkewedCombinerExample.class.getName());
		// @formatter:on
	}

	private void disclaimer() {
		System.out.println("Disclaimer...");
	}
}

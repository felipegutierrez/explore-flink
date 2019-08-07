package org.sense.flink.examples.stream;

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
		// Point point = new Point(725140.37, 4371855.492); // id=3, district=Extramurs
		Point point = new Point(726777.707, 4369824.436); // id=10, district=Quatre Carreres
		double distance = 1000.0; // distance in meters
		long districtId = 10;

		// Sources -> add synthetic data -> filter
		DataStream<ValenciaItem> streamTrafficJam = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, Time.minutes(5).toMilliseconds())).name(ValenciaItemConsumer.class.getName())
				.map(new ValenciaItemDistrictMap()).name(ValenciaItemDistrictMap.class.getName())
				.flatMap(new ValenciaItemSyntheticData(ValenciaItemType.TRAFFIC_JAM, point, distance)).name(ValenciaItemSyntheticData.class.getName())
				.filter(new ValenciaItemFilter(ValenciaItemType.TRAFFIC_JAM)).name(ValenciaItemFilter.class.getName())
				;

		DataStream<ValenciaItem> streamAirPollution = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, Time.minutes(30).toMilliseconds())).name(ValenciaItemConsumer.class.getName())
				.map(new ValenciaItemDistrictMap()).name(ValenciaItemDistrictMap.class.getName())
				.flatMap(new ValenciaItemSyntheticData(ValenciaItemType.AIR_POLLUTION, point, distance, districtId)).name(ValenciaItemSyntheticData.class.getName())
				.filter(new ValenciaItemFilter(ValenciaItemType.AIR_POLLUTION)).name(ValenciaItemFilter.class.getName())
				;

		// Join -> Print
		streamTrafficJam.join(streamAirPollution)
				.where(new ValenciaItemDistrictSelector())
 				.equalTo(new ValenciaItemDistrictSelector())
		 		.window(TumblingEventTimeWindows.of(Time.seconds(20)))
		 		.apply(new TrafficPollutionByDistrictJoinFunction())
		 		.print()
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

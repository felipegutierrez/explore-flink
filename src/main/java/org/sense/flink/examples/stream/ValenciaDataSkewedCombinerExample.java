package org.sense.flink.examples.stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.sense.flink.examples.stream.udf.impl.ValenciaPollutionAdminLevelMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaPollutionAssignDistrictAsKeyMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaPollutionSyntheticData;
import org.sense.flink.examples.stream.udf.impl.ValenciaTrafficAdminLevelMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaTrafficAssignDistrictAsKeyMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaTrafficJamSyntheticData;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaPollution;
import org.sense.flink.pojo.ValenciaTraffic;
import org.sense.flink.source.ValenciaPollutionConsumer;
import org.sense.flink.source.ValenciaTrafficJamConsumer;

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
		DataStream<Tuple2<Long, ValenciaTraffic>> streamTrafficJam = env
				.addSource(new ValenciaTrafficJamConsumer()).name(ValenciaTrafficJamConsumer.class.getName())
				.map(new ValenciaTrafficAdminLevelMap()).name(ValenciaTrafficAdminLevelMap.class.getName())
				.flatMap(new ValenciaTrafficJamSyntheticData(point, distance)).name(ValenciaTrafficJamSyntheticData.class.getName())
				// .filter(new ValenciaTrafficFilter()).name(ValenciaTrafficFilter.class.getName())
				.map(new ValenciaTrafficAssignDistrictAsKeyMap()).name(ValenciaTrafficAssignDistrictAsKeyMap.class.getName())
				;
		DataStream<Tuple2<Long, ValenciaPollution>> streamAirPollution = env
				.addSource(new ValenciaPollutionConsumer()).name(ValenciaPollutionConsumer.class.getName())
				.map(new ValenciaPollutionAdminLevelMap()).name(ValenciaPollutionAdminLevelMap.class.getName())
				.flatMap(new ValenciaPollutionSyntheticData(point, distance, districtId)).name(ValenciaPollutionSyntheticData.class.getName())
				// .filter(new ValenciaPollutionFilter()).name(ValenciaPollutionFilter.class.getName())
				.map(new ValenciaPollutionAssignDistrictAsKeyMap()).name(ValenciaPollutionAssignDistrictAsKeyMap.class.getName())
				;
		// DataStream<ValenciaItem> streamNoise = env
		// .addSource(new ValenciaNoiseConsumer()).name(ValenciaNoiseConsumer.class.getName());

		// Combine -> Print
		// streamTrafficJam.union(streamAirPollution).print();
		streamTrafficJam.print();
		streamAirPollution.print();

		env.execute(ValenciaDataSkewedCombinerExample.class.getName());
		// @formatter:on
	}

	private void disclaimer() {
		System.out.println("Disclaimer...");
	}
}

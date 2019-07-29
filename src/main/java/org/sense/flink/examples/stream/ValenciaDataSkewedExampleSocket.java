package org.sense.flink.examples.stream;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.sense.flink.examples.stream.udf.impl.ValenciaTrafficAdminLevelMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaPollutionFilter;
import org.sense.flink.examples.stream.udf.impl.ValenciaPollutionSyntheticData;
import org.sense.flink.examples.stream.udf.impl.ValenciaTrafficFilter;
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
public class ValenciaDataSkewedExampleSocket {

	public static void main(String[] args) throws Exception {
		new ValenciaDataSkewedExampleSocket();
	}

	public ValenciaDataSkewedExampleSocket() throws Exception {
		disclaimer();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// @formatter:off
		// Static coordinate to create synthetic data
		Point point = new Point(725737.858, 4370806.847);
		double distance = 100.0;

		// Sources -> add synthetic data -> filter
		DataStream<ValenciaTraffic> streamTrafficJam = env
				.addSource(new ValenciaTrafficJamConsumer()).name(ValenciaTrafficJamConsumer.class.getName())
				.flatMap(new ValenciaTrafficJamSyntheticData(point, distance)).name(ValenciaTrafficJamSyntheticData.class.getName())
				.filter(new ValenciaTrafficFilter()).name(ValenciaTrafficFilter.class.getName());
		DataStream<ValenciaPollution> streamAirPollution = env
				.addSource(new ValenciaPollutionConsumer()).name(ValenciaPollutionConsumer.class.getName())
				.flatMap(new ValenciaPollutionSyntheticData(point, distance)).name(ValenciaPollutionSyntheticData.class.getName())
				.filter(new ValenciaPollutionFilter()).name(ValenciaPollutionFilter.class.getName());
		// DataStream<ValenciaNoise> streamNoise = env
		// .addSource(new ValenciaNoiseConsumer()).name(ValenciaNoiseConsumer.class.getName());

		// Join -> Print
		streamTrafficJam.join(streamAirPollution)
				.where(null)
				.equalTo(null)
				;
		// streamTrafficJam.print();
		// streamAirPollution.print();
		
		env.execute(ValenciaDataSkewedExampleSocket.class.getName());
		// @formatter:on
	}

	private void disclaimer() {
		System.out.println("Disclaimer...");
	}
}

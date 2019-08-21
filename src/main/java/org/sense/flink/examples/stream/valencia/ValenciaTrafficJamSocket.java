package org.sense.flink.examples.stream.valencia;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.source.ValenciaItemConsumer;
import org.sense.flink.util.ValenciaItemType;

/**
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaTrafficJamSocket {

	public static void main(String[] args) throws Exception {
		new ValenciaTrafficJamSocket();
	}

	public ValenciaTrafficJamSocket() throws Exception {
		boolean offlineData = true;
		boolean collectWithTimestamp = true;
		boolean skewedDataInjection = true;

		// @formatter:off
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		env.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, Time.seconds(20).toMilliseconds(), collectWithTimestamp, offlineData, skewedDataInjection))
				.name(ValenciaItemConsumer.class.getName())
			.map(new ValenciaItemDistrictMap()).name(ValenciaItemDistrictMap.class.getName())
			.print();
		// @formatter:on

		env.execute(ValenciaTrafficJamSocket.class.getName());
	}
}

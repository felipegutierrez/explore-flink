package org.sense.flink.examples.stream.valencia;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.source.ValenciaItemConsumer;
import org.sense.flink.util.ValenciaItemType;

/**
 * use zone 30 and the coordinates of the open-data portal
 * https://www.engineeringtoolbox.com/utm-latitude-longitude-d_1370.html
 * 
 * or use:
 * 
 * EPSG:32630 Name:WGS 84 / UTM zone 30N (https://epsg.io/32630)
 * 
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaNoiseSocket {

	public static void main(String[] args) throws Exception {
		new ValenciaNoiseSocket();
	}

	public ValenciaNoiseSocket() throws Exception {
		boolean offlineData = true;
		boolean collectWithTimestamp = true;
		boolean skewedDataInjection = true;
		long frequencyMilliSeconds = Time.seconds(20).toMilliseconds();

		// @formatter:off
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		env.addSource(new ValenciaItemConsumer(ValenciaItemType.NOISE, frequencyMilliSeconds, collectWithTimestamp, offlineData, skewedDataInjection, Long.MAX_VALUE, false))
				.name(ValenciaItemConsumer.class.getSimpleName())
			.print();
		// @formatter:on

		env.execute(ValenciaNoiseSocket.class.getSimpleName());
	}
}

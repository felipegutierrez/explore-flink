package org.sense.flink.examples.stream;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.sense.flink.source.ValenciaNoiseConsumer;

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

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// @formatter:off
		env.addSource(new ValenciaNoiseConsumer(ValenciaNoiseConsumer.VALENCIA_NOISE_URL))
			.print();

		env.execute(ValenciaNoiseSocket.class.getName());
		// @formatter:on
	}
}

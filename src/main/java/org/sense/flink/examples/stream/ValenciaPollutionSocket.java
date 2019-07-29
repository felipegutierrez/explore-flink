package org.sense.flink.examples.stream;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.sense.flink.examples.stream.udf.impl.ValenciaPollutionAdminLevelMap;
import org.sense.flink.source.ValenciaPollutionConsumer;

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
public class ValenciaPollutionSocket {

	public static void main(String[] args) throws Exception {
		new ValenciaPollutionSocket();
	}

	public ValenciaPollutionSocket() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// @formatter:off
		env.addSource(new ValenciaPollutionConsumer())
			.map(new ValenciaPollutionAdminLevelMap())
			.print();

		env.execute(ValenciaPollutionSocket.class.getName());
		// @formatter:on
	}
}

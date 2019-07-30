package org.sense.flink.examples.stream;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.sense.flink.examples.stream.udf.impl.ValenciaTrafficAdminLevelMap;
import org.sense.flink.source.ValenciaTrafficJamConsumer;

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

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// @formatter:off
		env.addSource(new ValenciaTrafficJamConsumer()).name(ValenciaTrafficJamConsumer.class.getName())
			.map(new ValenciaTrafficAdminLevelMap()).name(ValenciaTrafficAdminLevelMap.class.getName())
			.print();

		env.execute(ValenciaTrafficJamSocket.class.getName());
		// @formatter:on
	}
}

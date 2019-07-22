package org.sense.flink.examples.stream;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
		env.addSource(new ValenciaTrafficJamConsumer(ValenciaTrafficJamConsumer.VALENCIA_TRAFFIC_JAM_URL))
			.print();

		env.execute("URL download job");
		// @formatter:on
	}
}

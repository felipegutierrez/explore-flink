package org.sense.flink.examples.stream;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.sense.flink.source.ValenciaPollutionConsumer;

/**
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
		env.addSource(new ValenciaPollutionConsumer(ValenciaPollutionConsumer.VALENCIA_POLLUTION_URL))
			.print();

		env.execute(ValenciaPollutionSocket.class.getName());
		// @formatter:on
	}
}

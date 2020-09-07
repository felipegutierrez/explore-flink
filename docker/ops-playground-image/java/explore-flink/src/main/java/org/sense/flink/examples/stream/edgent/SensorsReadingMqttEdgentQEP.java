package org.sense.flink.examples.stream.edgent;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.sense.flink.mqtt.FlinkMqttConsumer;
import org.sense.flink.mqtt.MqttMessage;

public class SensorsReadingMqttEdgentQEP {

	public SensorsReadingMqttEdgentQEP() throws Exception {

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// obtain execution environment, run this example in "ingestion time"
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<MqttMessage> temperatureStream = env.addSource(new FlinkMqttConsumer("topic-edgent"));

		temperatureStream.print();

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("SensorsReadingMqttEdgentQEP");
	}
}

package org.sense.flink.examples.stream.kafka;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * Follow the instructions at
 * https://www.ververica.com/blog/kafka-flink-a-practical-how-to to launch the
 * Kafka broker
 * 
 * <pre>
 * cd kafka_2.10-0.8.2.1
 * # start zookeeper server
 * 
 * ./bin/zookeeper-server-start.sh ./config/zookeeper.properties
 * # start broker
 * 
 * ./bin/kafka-server-start.sh ./config/server.properties 
 * # create topic “test”
 * 
 * ./bin/kafka-topics.sh --create --topic test --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * # consume from the topic using the console producer
 * 
 * ./bin/kafka-console-consumer.sh --topic test --zookeeper localhost:2181
 * # produce something into the topic (write something and hit enter)
 * 
 * ./bin/kafka-console-producer.sh --topic test --broker-list localhost:9092
 * </pre>
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class KafkaConsumerQuery {

	public KafkaConsumerQuery() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");

		FlinkKafkaConsumer myConsumer = new FlinkKafkaConsumer(java.util.regex.Pattern.compile("test"),
				new MySimpleStringSchema(), properties);

		DataStream<String> stream = env.addSource(myConsumer);
		stream.print();

		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(KafkaConsumerQuery.class.getSimpleName());
	}

	private static class MySimpleStringSchema extends SimpleStringSchema {
		private static final long serialVersionUID = 1L;
		private final String SHUTDOWN = "SHUTDOWN";

		@Override
		public String deserialize(byte[] message) {

			return super.deserialize(message);
		}

		@Override
		public boolean isEndOfStream(String nextElement) {
			if (SHUTDOWN.equalsIgnoreCase(nextElement)) {
				return true;
			}
			return super.isEndOfStream(nextElement);
		}
	}

	public static void main(String[] args) throws Exception {
		new KafkaConsumerQuery();
	}
}

package org.sense.flink.examples.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.sense.flink.mqtt.FlinkMqttConsumer;
import org.sense.flink.mqtt.MqttMessage;

/**
 * On the terminal execute "nc -lk 9000", run this class and type words back on
 * the terminal
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class WordCountMqttFilterQEP {

	public WordCountMqttFilterQEP() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<String, Integer>> dataStream = env.addSource(new FlinkMqttConsumer("topic"))
				.flatMap(new SplitterFlatMapMqtt()).keyBy(0) // select the first value as a key
				.sum(1) // reduce to sum all values with same key
				.filter(word -> word.f1 >= 3) // use simple filter
		;

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		dataStream.print();

		env.execute("WordCountMqttFilterQEP");
	}

	public static class SplitterFlatMapMqtt implements FlatMapFunction<MqttMessage, Tuple2<String, Integer>> {
		@Override
		public void flatMap(MqttMessage sentence, Collector<Tuple2<String, Integer>> out) throws Exception {

			String[] tokens = sentence.getPayload().toLowerCase().split(" ");

			for (String word : tokens) {
				out.collect(new Tuple2<String, Integer>(word, 1));
			}
		}
	}
}

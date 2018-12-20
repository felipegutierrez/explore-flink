package org.sense.flink.examples.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.mqtt.FlinkMqttConsumer;
import org.sense.flink.mqtt.MqttMessage;

public class SensorsDynamicFilterMqttEdgentQEP {

	public SensorsDynamicFilterMqttEdgentQEP() throws Exception {

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// obtain execution environment, run this example in "ingestion time"
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<MqttMessage> temperatureStream = env.addSource(new FlinkMqttConsumer("topic-edgent"));
		DataStream<Tuple2<Double, Double>> parameterStream = env.addSource(new FlinkMqttConsumer("topic-parameter"))
				.map(new ParameterMapper());

		DataStream<MqttMessage> filteredStream = temperatureStream.connect(parameterStream.broadcast())
				.flatMap(new DynamicFilterCoFlatMapper());

		filteredStream.print();

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("SensorsDynamicFilterMqttEdgentQEP");
	}

	public static class DynamicFilterCoFlatMapper
			implements CoFlatMapFunction<MqttMessage, Tuple2<Double, Double>, MqttMessage> {

		private static final long serialVersionUID = -8634404029870404558L;
		private Tuple2<Double, Double> range = new Tuple2<Double, Double>(-1000.0, 1000.0);

		@Override
		public void flatMap1(MqttMessage value, Collector<MqttMessage> out) throws Exception {

			double payload = Double.parseDouble(value.getPayload());

			if (payload >= this.range.f0 && payload <= this.range.f1) {
				out.collect(value);
			}
		}

		@Override
		public void flatMap2(Tuple2<Double, Double> value, Collector<MqttMessage> out) throws Exception {
			this.range = value;
		}
	}

	public static class ParameterMapper implements MapFunction<MqttMessage, Tuple2<Double, Double>> {

		private static final long serialVersionUID = 7322348505833012711L;

		@Override
		public Tuple2<Double, Double> map(MqttMessage value) throws Exception {
			String[] array = value.getPayload().split(",");
			double min = Double.parseDouble(array[0]);
			double max = Double.parseDouble(array[1]);
			return new Tuple2<Double, Double>(min, max);
		}
	}
}

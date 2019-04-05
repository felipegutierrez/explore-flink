package org.sense.flink.examples.stream;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.udfs.SensorKeySelector;
import org.sense.flink.examples.stream.udfs.SensorTypeMapper;
import org.sense.flink.mqtt.CompositeKeySensorType;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.mqtt.MqttSensorConsumer;

public class MqttSensorExpensiveShuffleDAG {

	public static void main(String[] args) throws Exception {
		new MqttSensorExpensiveShuffleDAG("192.168.56.20");
	}

	public MqttSensorExpensiveShuffleDAG(String ipAddressSource01) throws Exception {

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// obtain execution environment, run this example in "ingestion time"
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		// the period of the emitted markers is 5 milliseconds
		env.getConfig().setLatencyTrackingInterval(5L);

		DataStream<MqttSensor> streamStation01 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, "topic-station-01"))
				.name(MqttSensorExpensiveShuffleDAG.class.getSimpleName() + "-topic-station-01");
		DataStream<MqttSensor> streamStation02 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, "topic-station-02"))
				.name(MqttSensorExpensiveShuffleDAG.class.getSimpleName() + "-topic-station-02");

		// @formatter:off
		// 1 - Union of both streams
		// 2 - Partition by key using the sensor type
		// 3 - Aggregate by key to process the sum or average based on the sensor type over a window of 5 seconds
		// 4 - print the results
		DataStream<Tuple3<CompositeKeySensorType, Double, Long>> streamStations = streamStation01.union(streamStation02)
				.map(new SensorTypeMapper()).name(SensorTypeMapper.class.getSimpleName())
				.setParallelism(2)
				.keyBy(new SensorKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
				.aggregate(new SensorTypeAggregator()).name(SensorTypeAggregator.class.getSimpleName())
				.setParallelism(2)
				;

		// print the results with a single thread, rather than in parallel
		streamStations.print().setParallelism(1);
		// @formatter:on

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute(MqttSensorExpensiveShuffleDAG.class.getSimpleName());
	}

	/**
	 * This class is an aggregator which has different computation depending on the
	 * sensor type. For TEMP and LIFT_V we compute the average. For COUNT_PE,
	 * COUNT_TI, and COUNT_TR we compute the sum.
	 * 
	 * @author Felipe Oliveira Gutierrez
	 */
	public static class SensorTypeAggregator implements
			AggregateFunction<Tuple2<CompositeKeySensorType, MqttSensor>, Tuple3<CompositeKeySensorType, Double, Long>, Tuple3<CompositeKeySensorType, Double, Long>> {
		private static final long serialVersionUID = -6819758633805621166L;

		@Override
		public Tuple3<CompositeKeySensorType, Double, Long> createAccumulator() {
			return new Tuple3<CompositeKeySensorType, Double, Long>(new CompositeKeySensorType(), 0.0, 0L);
		}

		@Override
		public Tuple3<CompositeKeySensorType, Double, Long> add(Tuple2<CompositeKeySensorType, MqttSensor> valueToAdd,
				Tuple3<CompositeKeySensorType, Double, Long> accumulator) {
			// this.meter.markEvent();
			// this.counter.inc();
			System.out.println("Aggregator: " + valueToAdd);
			return new Tuple3<CompositeKeySensorType, Double, Long>(valueToAdd.f0,
					accumulator.f1 + valueToAdd.f1.getValue(), accumulator.f2 + 1L);
		}

		@Override
		public Tuple3<CompositeKeySensorType, Double, Long> getResult(
				Tuple3<CompositeKeySensorType, Double, Long> accumulator) {

			if (accumulator.f0.getSensorType().equals("TEMP") || accumulator.f0.getSensorType().equals("LIFT_V")) {
				return new Tuple3<CompositeKeySensorType, Double, Long>(accumulator.f0, accumulator.f1 / accumulator.f2,
						accumulator.f2);
			} else if (accumulator.f0.getSensorType().equals("COUNT_PE")
					|| accumulator.f0.getSensorType().equals("COUNT_TI")
					|| accumulator.f0.getSensorType().equals("COUNT_TR")) {
				return new Tuple3<CompositeKeySensorType, Double, Long>(accumulator.f0, accumulator.f1, accumulator.f2);
			} else {
				System.err.println("Sensor type not defined.");
				return null;
			}
		}

		@Override
		public Tuple3<CompositeKeySensorType, Double, Long> merge(Tuple3<CompositeKeySensorType, Double, Long> a,
				Tuple3<CompositeKeySensorType, Double, Long> b) {
			return new Tuple3<CompositeKeySensorType, Double, Long>(a.f0, a.f1 + b.f1, a.f2 + b.f2);
		}
	}
}

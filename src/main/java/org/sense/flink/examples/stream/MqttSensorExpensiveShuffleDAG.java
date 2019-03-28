package org.sense.flink.examples.stream;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.mqtt.CompositeKeySensorType;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.mqtt.MqttSensorConsumer;

public class MqttSensorExpensiveShuffleDAG {

	public MqttSensorExpensiveShuffleDAG(String ipAddressSource01) throws Exception {

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// obtain execution environment, run this example in "ingestion time"
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		DataStream<MqttSensor> streamStation01 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, "topic-station-01"));
		DataStream<MqttSensor> streamStation02 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, "topic-station-02"));

		// @formatter:off
		// 1 - Union of both streams
		// 2 - Partition by key using the sensor type
		// 3 - Aggregate by key to process the sum or average based on the sensor type over a window of 5 seconds
		// 4 - print the results
		// DataStream<MqttSensor> streamStations =
		streamStation01.union(streamStation02)
				.map(new SensorTypeMapper())
				.keyBy(new MyKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
				.aggregate(new SensorTypeAggregator())
				// .map(new PrinterMapper())
				.print()
				;
		// @formatter:on

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("MqttSensorExpensiveShuffleDAG");
	}

	public static class SensorTypeMapper
			implements MapFunction<MqttSensor, Tuple2<CompositeKeySensorType, MqttSensor>> {
		private static final long serialVersionUID = -4080196110995184486L;

		@Override
		public Tuple2<CompositeKeySensorType, MqttSensor> map(MqttSensor value) throws Exception {
			// every sensor key: sensorId, sensorType, platformId, platformType, stationId
			// Integer sensorId = value.getKey().f0;
			String sensorType = value.getKey().f1;
			Integer platformId = value.getKey().f2;
			// String platformType = value.getKey().f3;
			Integer stationId = value.getKey().f4;
			CompositeKeySensorType compositeKey = new CompositeKeySensorType(stationId, platformId, sensorType);
			return Tuple2.of(compositeKey, value);
		}
	}

	public static class MyKeySelector
			implements KeySelector<Tuple2<CompositeKeySensorType, MqttSensor>, CompositeKeySensorType> {
		private static final long serialVersionUID = -1850482738358805000L;

		@Override
		public CompositeKeySensorType getKey(Tuple2<CompositeKeySensorType, MqttSensor> value) throws Exception {
			return value.f0;
		}
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

	public static class PrinterMapper implements
			MapFunction<Tuple3<CompositeKeySensorType, Double, Long>, Tuple3<CompositeKeySensorType, Double, Long>> {
		private static final long serialVersionUID = -3828648840274206175L;

		@Override
		public Tuple3<CompositeKeySensorType, Double, Long> map(Tuple3<CompositeKeySensorType, Double, Long> value)
				throws Exception {
			System.out.println(value);
			return value;
		}
	}
}

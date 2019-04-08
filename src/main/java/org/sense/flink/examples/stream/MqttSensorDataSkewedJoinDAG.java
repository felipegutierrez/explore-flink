package org.sense.flink.examples.stream;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.udfs.SensorSkewedJoinFunction;
import org.sense.flink.examples.stream.udfs.StationPlatformKeySelector;
import org.sense.flink.examples.stream.udfs.StationPlatformMapper;
import org.sense.flink.mqtt.CompositeKeySensorType;
import org.sense.flink.mqtt.CompositeKeyStationPlatform;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.mqtt.MqttSensorConsumer;
import org.sense.flink.mqtt.MqttStringPublisher;

public class MqttSensorDataSkewedJoinDAG {

	private final String topic = "topic-data-skewed-join";

	public static void main(String[] args) throws Exception {
		new MqttSensorDataSkewedJoinDAG("192.168.56.20", "192.168.56.1");
	}

	public MqttSensorDataSkewedJoinDAG(String ipAddressSource01, String ipAddressSink) throws Exception {

		// @formatter:off
		System.out.println("App 14 selected (Complex shuffle with aggregation over a window)");
		System.out.println("Use [./bin/flink run examples/explore-flink.jar 14 " + ipAddressSource01 + " " + ipAddressSink + " -c] to run this program on the Flink standalone-cluster");
		System.out.println("Consuming values from 2 MQTT topics");
		System.out.println("Use 'mosquitto_sub -h " + ipAddressSource01 + " -t " + topic + "' in order to consume data from this job.");
		// @formatter:on

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// obtain execution environment, run this example in "ingestion time"
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		// the period of the emitted markers is 5 milliseconds
		env.getConfig().setLatencyTrackingInterval(5L);

		DataStream<MqttSensor> streamTrainsStation01 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, "topic-station-01-trains"))
				.name(MqttSensorDataSkewedJoinDAG.class.getSimpleName() + "-topic-station-01-trains");
		DataStream<MqttSensor> streamTicketsStation01 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, "topic-station-01-tickets"))
				.name(MqttSensorDataSkewedJoinDAG.class.getSimpleName() + "-topic-station-01-tickets");

		// @formatter:off
		// 1 - Union of both streams
		// 2 - Partition by key using the sensor type
		// 3 - Aggregate by key to process the sum or average based on the sensor type over a window of 5 seconds
		// 4 - print the results
		DataStream<Tuple2<CompositeKeyStationPlatform, MqttSensor>> mappedTrainsStation01 = streamTrainsStation01
				.map(new StationPlatformMapper()).name(StationPlatformMapper.class.getSimpleName() + "-trains01");
		DataStream<Tuple2<CompositeKeyStationPlatform, MqttSensor>> mappedTicketsStation01 = streamTicketsStation01
				.map(new StationPlatformMapper()).name(StationPlatformMapper.class.getSimpleName() + "-tickets01");
		
		mappedTrainsStation01.join(mappedTicketsStation01)
				.where(new StationPlatformKeySelector())
				.equalTo(new StationPlatformKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
				.apply(new SensorSkewedJoinFunction())
				.addSink(new MqttStringPublisher(ipAddressSink, topic)).name(MqttStringPublisher.class.getSimpleName())
				;

		// @formatter:on

		System.out.println("........................ ");
		System.out.println("ExecutionPlan: " + env.getExecutionPlan());
		System.out.println("........................ ");

		env.execute(MqttSensorDataSkewedJoinDAG.class.getSimpleName());
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

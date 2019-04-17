package org.sense.flink.examples.stream;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.operators.CountBundleTrigger;
import org.sense.flink.examples.stream.operators.MapBundleFunction;
import org.sense.flink.examples.stream.operators.MapBundleFunctionImpl;
import org.sense.flink.examples.stream.operators.MapStreamBundleOperator;
import org.sense.flink.examples.stream.udfs.SensorTypePlatformStationMapper;
import org.sense.flink.examples.stream.udfs.StationPlatformKeySelector;
import org.sense.flink.examples.stream.udfs.StationPlatformMapper;
import org.sense.flink.examples.stream.udfs.StationPlatformRichWindowFunction;
import org.sense.flink.mqtt.CompositeKeySensorTypePlatformStation;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.mqtt.MqttSensorConsumer;
import org.sense.flink.mqtt.MqttStationPlatformPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttSensorDataSkewedCombinerByKeySkewedDAG {

	private static final Logger logger = LoggerFactory.getLogger(MqttSensorDataSkewedCombinerByKeySkewedDAG.class);

	private final String topic = "topic-data-skewed-join";
	private final String topic_station_01_trains = "topic-station-01-trains";
	private final String topic_station_01_tickets = "topic-station-01-tickets";
	private final String topic_station_02_trains = "topic-station-02-trains";
	private final String topic_station_02_tickets = "topic-station-02-tickets";

	private final String metricSensorMapper = "SensorTypeStationPlatformMapper";
	private final String metricCombiner = "SensorTypeStationPlatformCombiner";
	private final String metricMapper = "StationPlatformMapper";
	private final String metricWindowFunction = "StationPlatformRichWindowFunction";
	private final String metricSkewedMapper = "StationPlatformSkewedMapper";
	private final String metricSinkFunction = "SinkFunction";

	public static void main(String[] args) throws Exception {
		// newMqttSensorDataSkewedCombinerByKeySkewedDAG("192.168.56.20","192.168.56.1");
		new MqttSensorDataSkewedCombinerByKeySkewedDAG("192.168.56.20", "127.0.0.1");
	}

	public MqttSensorDataSkewedCombinerByKeySkewedDAG(String ipAddressSource01, String ipAddressSink) throws Exception {

		System.out.println("App 18 selected (Complex shuffle with aggregation over a window with keyBy skewed)");
		System.out.println("This Flink application fix the skewed key of the App 15");

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// obtain execution environment, run this example in "ingestion time"
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// Data sources
		DataStream<MqttSensor> streamTrainsStation01 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, topic_station_01_trains))
				.name(MqttSensorConsumer.class.getSimpleName() + "-" + topic_station_01_trains);
		DataStream<MqttSensor> streamTicketsStation01 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, topic_station_01_tickets))
				.name(MqttSensorConsumer.class.getSimpleName() + "-" + topic_station_01_tickets);
		DataStream<MqttSensor> streamTrainsStation02 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, topic_station_02_trains))
				.name(MqttSensorConsumer.class.getSimpleName() + "-" + topic_station_02_trains);
		DataStream<MqttSensor> streamTicketsStation02 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, topic_station_02_tickets))
				.name(MqttSensorConsumer.class.getSimpleName() + "-" + topic_station_02_tickets);

		// @formatter:off
		// Create my own operator using AbstractUdfStreamOperator
		MapBundleFunction<CompositeKeySensorTypePlatformStation, MqttSensor, Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>, MqttSensor> myMapBundleFunction = new MapBundleFunctionImpl();
		CountBundleTrigger<Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>> bundleTrigger = 
				new CountBundleTrigger<Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>>(500);
		KeySelector<Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>, CompositeKeySensorTypePlatformStation> keyBundleSelector = 
				(KeySelector<Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>, CompositeKeySensorTypePlatformStation>) value -> value.f0;
		TypeInformation<MqttSensor> info = TypeInformation.of(MqttSensor.class);

		streamTrainsStation01.union(streamTrainsStation02).union(streamTicketsStation01).union(streamTicketsStation02)
				.map(new SensorTypePlatformStationMapper(metricSensorMapper)).name(metricSensorMapper)
				// .setParallelism(4)
				.transform(metricCombiner, info, new MapStreamBundleOperator<>(myMapBundleFunction, bundleTrigger, keyBundleSelector)).name(metricCombiner)
				// .setParallelism(4)
				.map(new StationPlatformMapper(metricMapper)).name(metricMapper)
				// .setParallelism(4)
				.keyBy(new StationPlatformKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
				.apply(new StationPlatformRichWindowFunction(metricWindowFunction)).name(metricWindowFunction)
				// .setParallelism(4)
				.map(new StationPlatformMapper(metricSkewedMapper)).name(metricSkewedMapper)
				.addSink(new MqttStationPlatformPublisher(ipAddressSink, topic)).name(metricSinkFunction)
				;
		// @formatter:on

		System.out.println("........................ ");
		System.out.println("ExecutionPlan: " + env.getExecutionPlan());
		System.out.println("........................ ");

		env.execute(MqttSensorDataSkewedCombinerByKeySkewedDAG.class.getSimpleName());
	}
}

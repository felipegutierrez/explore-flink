package org.sense.flink.examples.stream;

import static org.sense.flink.util.MetricLabels.METRIC_SENSOR_COMBINER;
import static org.sense.flink.util.MetricLabels.METRIC_SENSOR_STATION_PLATFORM_MAPPER;
import static org.sense.flink.util.MetricLabels.METRIC_SENSOR_MAPPER;
import static org.sense.flink.util.MetricLabels.METRIC_SENSOR_SINK;
import static org.sense.flink.util.MetricLabels.METRIC_SENSOR_STATION_PLATFORM_SKEWED_MAPPER;
import static org.sense.flink.util.MetricLabels.METRIC_SENSOR_STATION_PLATFORM_WINDOW;
import static org.sense.flink.util.SensorTopics.TOPIC_STATION_01_TICKETS;
import static org.sense.flink.util.SensorTopics.TOPIC_STATION_01_TRAINS;
import static org.sense.flink.util.SensorTopics.TOPIC_STATION_02_TICKETS;
import static org.sense.flink.util.SensorTopics.TOPIC_STATION_02_TRAINS;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.operator.impl.MapStreamBundleOperatorDynamic;
import org.sense.flink.examples.stream.trigger.impl.CountBundleTriggerDynamic;
import org.sense.flink.examples.stream.udf.MapBundleFunction;
import org.sense.flink.examples.stream.udf.impl.MapBundleFunctionImpl;
import org.sense.flink.examples.stream.udf.impl.SensorTypePlatformStationMapper;
import org.sense.flink.examples.stream.udf.impl.StationPlatformKeySelector;
import org.sense.flink.examples.stream.udf.impl.StationPlatformMapper;
import org.sense.flink.examples.stream.udf.impl.StationPlatformRichWindowFunction;
import org.sense.flink.mqtt.CompositeKeySensorTypePlatformStation;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.mqtt.MqttSensorConsumer;
import org.sense.flink.mqtt.MqttStationPlatformPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttSensorDataSkewedCombinerDynamicByKeySkewedDAG {

	private static final Logger logger = LoggerFactory
			.getLogger(MqttSensorDataSkewedCombinerDynamicByKeySkewedDAG.class);

	private final String topic = "topic-data-skewed-join";

	public static void main(String[] args) throws Exception {
		// MqttSensorDataSkewedCombinerDynamicByKeySkewedDAG("192.168.56.20","192.168.56.1");
		new MqttSensorDataSkewedCombinerDynamicByKeySkewedDAG("127.0.0.1", "127.0.0.1");
	}

	public MqttSensorDataSkewedCombinerDynamicByKeySkewedDAG(String ipAddressSource01, String ipAddressSink)
			throws Exception {
		// @formatter:off
		System.out.println("App 18 selected (Complex shuffle with aggregation over a window with keyBy skewed)");
		System.out.println("This Flink application fix the skewed key of the App 15");
		System.out.println("Data consumer for the explore-rpi application 11 'java -jar target/explore-rpi.jar 11'");

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// obtain execution environment, run this example in "ingestion time"
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// Data sources
		DataStream<MqttSensor> streamTrainsStation01 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, TOPIC_STATION_01_TRAINS))
				.name(MqttSensorConsumer.class.getSimpleName() + "-" + TOPIC_STATION_01_TRAINS);
		DataStream<MqttSensor> streamTicketsStation01 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, TOPIC_STATION_01_TICKETS))
				.name(MqttSensorConsumer.class.getSimpleName() + "-" + TOPIC_STATION_01_TICKETS);
		DataStream<MqttSensor> streamTrainsStation02 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, TOPIC_STATION_02_TRAINS))
				.name(MqttSensorConsumer.class.getSimpleName() + "-" + TOPIC_STATION_02_TRAINS);
		DataStream<MqttSensor> streamTicketsStation02 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, TOPIC_STATION_02_TICKETS))
				.name(MqttSensorConsumer.class.getSimpleName() + "-" + TOPIC_STATION_02_TICKETS);

		// Create my own operator using AbstractUdfStreamOperator
		MapBundleFunction<CompositeKeySensorTypePlatformStation, MqttSensor, Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>, MqttSensor> myMapBundleFunction = new MapBundleFunctionImpl();
		CountBundleTriggerDynamic<CompositeKeySensorTypePlatformStation, Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>> bundleTrigger = 
				new CountBundleTriggerDynamic<CompositeKeySensorTypePlatformStation, Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>>();
		KeySelector<Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>, CompositeKeySensorTypePlatformStation> keyBundleSelector = 
				(KeySelector<Tuple2<CompositeKeySensorTypePlatformStation, MqttSensor>, CompositeKeySensorTypePlatformStation>) value -> value.f0;
		TypeInformation<MqttSensor> info = TypeInformation.of(MqttSensor.class);

		streamTrainsStation01.union(streamTrainsStation02).union(streamTicketsStation01).union(streamTicketsStation02)
				.map(new SensorTypePlatformStationMapper(METRIC_SENSOR_MAPPER)).name(METRIC_SENSOR_MAPPER)
				.transform(METRIC_SENSOR_COMBINER, info, new MapStreamBundleOperatorDynamic<>(myMapBundleFunction, bundleTrigger, keyBundleSelector)).name(METRIC_SENSOR_COMBINER)
				.map(new StationPlatformMapper(METRIC_SENSOR_STATION_PLATFORM_MAPPER)).name(METRIC_SENSOR_STATION_PLATFORM_MAPPER)
				.keyBy(new StationPlatformKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
				.apply(new StationPlatformRichWindowFunction(METRIC_SENSOR_STATION_PLATFORM_WINDOW)).name(METRIC_SENSOR_STATION_PLATFORM_WINDOW)
				.map(new StationPlatformMapper(METRIC_SENSOR_STATION_PLATFORM_SKEWED_MAPPER)).name(METRIC_SENSOR_STATION_PLATFORM_SKEWED_MAPPER)
				.addSink(new MqttStationPlatformPublisher(ipAddressSink, topic)).name(METRIC_SENSOR_SINK)
				;

		System.out.println("........................ ");
		System.out.println("ExecutionPlan: " + env.getExecutionPlan());
		System.out.println("........................ ");

		env.execute(MqttSensorDataSkewedCombinerDynamicByKeySkewedDAG.class.getSimpleName());
		// @formatter:on
	}
}

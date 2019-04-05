package org.sense.flink.examples.stream;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.udfs.MetricsProcessWindowFunction;
import org.sense.flink.examples.stream.udfs.PrinterSink;
import org.sense.flink.examples.stream.udfs.SensorKeySelector;
import org.sense.flink.examples.stream.udfs.SensorTypeMapper;
import org.sense.flink.examples.stream.udfs.SensorTypeReduce;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.mqtt.MqttSensorConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttSensorRandomPartitionByKeyDAG {

	final static Logger logger = LoggerFactory.getLogger(MqttSensorRandomPartitionByKeyDAG.class);

	public static void main(String[] args) throws Exception {
		new MqttSensorRandomPartitionByKeyDAG("192.168.56.20");
	}

	public MqttSensorRandomPartitionByKeyDAG(String ipAddressSource01) throws Exception {

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// obtain execution environment, run this example in "ingestion time"
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		// the period of the emitted markers is 5 milliseconds
		env.getConfig().setLatencyTrackingInterval(5L);

		DataStream<MqttSensor> streamStation01 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, "topic-station-01"), "source-topic-station-01")
				.name(MqttSensorConsumer.class.getSimpleName() + "-topic-station-01");
		DataStream<MqttSensor> streamStation02 = env
				.addSource(new MqttSensorConsumer(ipAddressSource01, "topic-station-02"), "source-topic-station-02")
				.name(MqttSensorConsumer.class.getSimpleName() + "-topic-station-02");

		// @formatter:off
		// DataStream<Tuple2<CompositeKeySensorType, MqttSensor>> streamStations = 
		streamStation01.union(streamStation02)
				.map(new SensorTypeMapper()).name(SensorTypeMapper.class.getSimpleName())
				.setParallelism(4)
				.shuffle()
				.keyBy(new SensorKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
				.reduce(new SensorTypeReduce(), new MetricsProcessWindowFunction()).name(SensorTypeReduce.class.getSimpleName())
				.setParallelism(4)
				.addSink(new PrinterSink()).name(PrinterSink.class.getSimpleName())
				.setParallelism(1)
				;
		// @formatter:on

		System.out.println("ExecutionPlan: " + env.getExecutionPlan());

		env.execute(MqttSensorRandomPartitionByKeyDAG.class.getSimpleName());
	}
}

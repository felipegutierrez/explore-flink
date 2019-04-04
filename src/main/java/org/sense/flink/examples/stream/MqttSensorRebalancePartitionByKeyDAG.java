package org.sense.flink.examples.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.mqtt.CompositeKeySensorType;
import org.sense.flink.mqtt.MqttSensor;
import org.sense.flink.mqtt.MqttSensorConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttSensorRebalancePartitionByKeyDAG {

	final static Logger logger = LoggerFactory.getLogger(MqttSensorRebalancePartitionByKeyDAG.class);

	public static void main(String[] args) throws Exception {
		new MqttSensorRebalancePartitionByKeyDAG("192.168.56.20");
	}

	public MqttSensorRebalancePartitionByKeyDAG(String ipAddressSource01) throws Exception {

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
				.setParallelism(6)
				.rebalance()
				.keyBy(new MyKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
				.reduce(new SensorTypeReduce()).name(SensorTypeReduce.class.getSimpleName())
				.setParallelism(6)
				.rebalance()
				.addSink(new PrinterSink()).name(PrinterSink.class.getSimpleName())
				.setParallelism(1)
				;
		// @formatter:on

		System.out.println("ExecutionPlan: " + env.getExecutionPlan());

		env.execute(MqttSensorRebalancePartitionByKeyDAG.class.getSimpleName());
	}

	private static class SensorTypeMapper
			implements MapFunction<MqttSensor, Tuple2<CompositeKeySensorType, MqttSensor>> {
		private static final long serialVersionUID = -6534035257028050767L;

		@Override
		public Tuple2<CompositeKeySensorType, MqttSensor> map(MqttSensor value) throws Exception {
			// this.meter.markEvent();
			// this.counter.inc();
			// every sensor key: sensorId, sensorType, platformId, platformType, stationId
			// Integer sensorId = value.getKey().f0;
			String sensorType = value.getKey().f1;
			Integer platformId = value.getKey().f2;
			// String platformType = value.getKey().f3;
			Integer stationId = value.getKey().f4;
			CompositeKeySensorType compositeKey = new CompositeKeySensorType(stationId, platformId, sensorType);

			logger.debug("Mapper: " + compositeKey + " - " + value);
			// System.out.println("Mapper: " + compositeKey + " - " + value);

			return Tuple2.of(compositeKey, value);
		}
	}

	private static class MyCustomPartitioner implements Partitioner<CompositeKeySensorType> {
		private static final long serialVersionUID = 7975348053137247432L;

		@Override
		public int partition(CompositeKeySensorType key, int numPartitions) {
			int partition = (key.getStationId() + key.getPlatformId()) % numPartitions;
			System.out.println("numPartitions[" + numPartitions + "]: " + partition);
			return partition;
		}
	}

	private static class MyKeySelector
			implements KeySelector<Tuple2<CompositeKeySensorType, MqttSensor>, CompositeKeySensorType> {
		private static final long serialVersionUID = -788050207886781964L;

		@Override
		public CompositeKeySensorType getKey(Tuple2<CompositeKeySensorType, MqttSensor> value) throws Exception {
			return value.f0;
		}
	}

	private static class SensorTypeReduce implements ReduceFunction<Tuple2<CompositeKeySensorType, MqttSensor>> {
		private static final long serialVersionUID = -6992113920095082919L;

		@Override
		public Tuple2<CompositeKeySensorType, MqttSensor> reduce(Tuple2<CompositeKeySensorType, MqttSensor> value1,
				Tuple2<CompositeKeySensorType, MqttSensor> value2) throws Exception {

			Tuple5<Integer, String, Integer, String, Integer> key = value1.f1.getKey();
			key.f0 = 0;
			Double sum = value1.f1.getValue() + value2.f1.getValue();

			logger.debug("Reducer: ");
			return Tuple2.of(value1.f0, new MqttSensor("", key, System.currentTimeMillis(), sum));
		}
	}

	private static class PrinterSink implements SinkFunction<Tuple2<CompositeKeySensorType, MqttSensor>> {
		private static final long serialVersionUID = -7230206725567490797L;

		@Override
		public void invoke(Tuple2<CompositeKeySensorType, MqttSensor> value) throws Exception {
			System.out.println(value);
		}
	}
}

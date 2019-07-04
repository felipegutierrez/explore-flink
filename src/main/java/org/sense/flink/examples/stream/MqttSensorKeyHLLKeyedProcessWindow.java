package org.sense.flink.examples.stream;

import static org.sense.flink.util.SensorTopics.TOPIC_STATION_01_PLAT_01_TICKETS;
import static org.sense.flink.util.SensorTopics.TOPIC_STATION_01_PLAT_01_TICKETS_CARDINALITY;
import static org.sense.flink.util.SensorTopics.TOPIC_STATION_01_PLAT_02_TICKETS;
import static org.sense.flink.util.SensorTopics.TOPIC_STATION_01_PLAT_02_TICKETS_CARDINALITY;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.mqtt.MqttSensorTupleConsumer;
import org.sense.flink.mqtt.MqttSensorTuplePublisher;
import org.sense.flink.util.SensorHyperLogLogState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttSensorKeyHLLKeyedProcessWindow {
	private static final Logger logger = LoggerFactory.getLogger(MqttSensorKeyHLLKeyedProcessWindow.class);

	public static void main(String[] args) throws Exception {
		// new MqttSensorKeyHLLKeyedProcessWindow("192.168.56.20","192.168.56.1");
		new MqttSensorKeyHLLKeyedProcessWindow("127.0.0.1", "127.0.0.1");
	}

	public MqttSensorKeyHLLKeyedProcessWindow(String ipAddressSource01, String ipAddressSink) throws Exception {

		System.out.println("App 21 selected (Estimate key cardinality with HyperLogLog)");
		// @formatter:off
		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Data sources
		DataStream<Tuple8<Integer, String, Integer, String, Integer, Long, Double, String>> streamTicketsStation01Plat01 = env
				.addSource(new MqttSensorTupleConsumer(ipAddressSource01, TOPIC_STATION_01_PLAT_01_TICKETS))
				.name(MqttSensorTupleConsumer.class.getSimpleName() + "-" + TOPIC_STATION_01_PLAT_01_TICKETS);
		DataStream<Tuple8<Integer, String, Integer, String, Integer, Long, Double, String>> streamTicketsStation01Plat02 = env
				.addSource(new MqttSensorTupleConsumer(ipAddressSource01, TOPIC_STATION_01_PLAT_02_TICKETS))
				.name(MqttSensorTupleConsumer.class.getSimpleName() + "-" + TOPIC_STATION_01_PLAT_02_TICKETS);

		streamTicketsStation01Plat01
				.keyBy(new NullByteKeySelector<Tuple8<Integer, String, Integer, String, Integer, Long, Double, String>>())
				.process(new TicketIdKeyedProcessFunction(60 * 1000))
				.addSink(new MqttSensorTuplePublisher(TOPIC_STATION_01_PLAT_01_TICKETS_CARDINALITY));
		streamTicketsStation01Plat02
				.keyBy(new NullByteKeySelector<Tuple8<Integer, String, Integer, String, Integer, Long, Double, String>>())
				.process(new TicketIdKeyedProcessFunction(60 * 1000))
				.addSink(new MqttSensorTuplePublisher(TOPIC_STATION_01_PLAT_02_TICKETS_CARDINALITY));

		// 11      | COUNT_PE  | 2         | CIT         | 1        | timestamp| 18   | Berlin-Paris
		// 11      | COUNT_TI  | 2         | CIT         | 1        | timestamp| 18   | Berlin-Paris
		// sensorId, sensorType, platformId, platformType, stationId, timestamp, value, trip
		// @formatter:on

		System.out.println("........................ ");
		System.out.println("ExecutionPlan: " + env.getExecutionPlan());
		System.out.println("........................ ");

		env.execute(MqttSensorKeyHLLKeyedProcessWindow.class.getSimpleName());
	}

	public static class TicketIdKeyedProcessFunction extends
			KeyedProcessFunction<Byte, Tuple8<Integer, String, Integer, String, Integer, Long, Double, String>, Tuple9<Integer, String, Integer, String, Integer, Long, Long, Long, String>> {
		private static final long serialVersionUID = 4514841115212858385L;
		// delay after which an alert flag is thrown
		private final long timeOut;
		// state to remember the last timer set
		private transient ValueState<SensorHyperLogLogState> hllState;

		public TicketIdKeyedProcessFunction(long timeOut) {
			this.timeOut = timeOut;
		}

		@Override
		public void open(Configuration conf) {
			// setup timer and HLL state
			ValueStateDescriptor<SensorHyperLogLogState> descriptorHLL = new ValueStateDescriptor<>("hllState",
					SensorHyperLogLogState.class);
			hllState = getRuntimeContext().getState(descriptorHLL);
		}

		@Override
		public void processElement(Tuple8<Integer, String, Integer, String, Integer, Long, Double, String> value,
				KeyedProcessFunction<Byte, Tuple8<Integer, String, Integer, String, Integer, Long, Double, String>, Tuple9<Integer, String, Integer, String, Integer, Long, Long, Long, String>>.Context ctx,
				Collector<Tuple9<Integer, String, Integer, String, Integer, Long, Long, Long, String>> out)
				throws Exception {
			// get current time and compute timeout time
			long currentTime = ctx.timerService().currentProcessingTime();
			long timeoutTime = currentTime + timeOut;

			// update HLL state
			SensorHyperLogLogState currentHLLState = hllState.value();
			if (currentHLLState == null) {
				// System.out.println("process: " + currentTime + " - " + timeoutTime);
				currentHLLState = new SensorHyperLogLogState();
				// register timer for timeout time
				ctx.timerService().registerProcessingTimeTimer(timeoutTime);
			}
			// the cardinality will be computed based on the value passed to this method
			currentHLLState.offer(Integer.valueOf(value.f6.intValue()), value);

			// remember timeout time
			currentHLLState.setLastTimer(timeoutTime);
			hllState.update(currentHLLState);
		}

		@Override
		public void onTimer(long timestamp,
				KeyedProcessFunction<Byte, Tuple8<Integer, String, Integer, String, Integer, Long, Double, String>, Tuple9<Integer, String, Integer, String, Integer, Long, Long, Long, String>>.OnTimerContext ctx,
				Collector<Tuple9<Integer, String, Integer, String, Integer, Long, Long, Long, String>> out)
				throws Exception {
			// check if this was the last timer we registered
			SensorHyperLogLogState currentHLLState = hllState.value();
			// System.out.println("onTimer: " + timestamp + " - " +
			// currentHLLState.getLastTimer() + " = " + (currentHLLState.getLastTimer() -
			// timestamp));
			// it was, so no data was received afterwards. fire an alert.
			out.collect(currentHLLState.getEstimative());
			hllState.clear();
		}
	}
}

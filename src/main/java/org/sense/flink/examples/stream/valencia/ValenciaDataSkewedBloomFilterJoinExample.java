package org.sense.flink.examples.stream.valencia;

import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_DISTRICT_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_LOOKUP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SIDE_OUTPUT;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SINK;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SOURCE;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.sense.flink.examples.stream.udf.impl.TrafficPollutionByDistrictJoinFunction;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictSelector;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemLookupKeySelector;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemOutputTag;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemProcessSideOutput;
import org.sense.flink.examples.stream.udf.impl.ValenciaLookupCoProcess;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.source.ValenciaItemConsumer;
import org.sense.flink.util.ValenciaItemType;

public class ValenciaDataSkewedBloomFilterJoinExample {
	private final String topic = "topic-valencia-data-skewed";

	public static void main(String[] args) throws Exception {
		new ValenciaDataSkewedBloomFilterJoinExample("127.0.0.1", "127.0.0.1");
	}

	public ValenciaDataSkewedBloomFilterJoinExample(String ipAddressSource01, String ipAddressSink) throws Exception {
		disclaimer();
		boolean offlineData = true;
		boolean collectWithTimestamp = true;
		boolean skewedDataInjection = true;
		boolean enableLookupKeys = true;
		long trafficFrequency = Time.seconds(10).toMilliseconds();
		long pollutionFrequency = Time.seconds(20).toMilliseconds();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// @formatter:off
		// Side outputs
		OutputTag<Tuple2<ValenciaItemType, Long>> outputTagTraffic = new ValenciaItemOutputTag("side-output-traffic");
		OutputTag<Tuple2<ValenciaItemType, Long>> outputTagPollution = new ValenciaItemOutputTag("side-output-pollution");

		// Sources -> add synthetic data -> filter
		SingleOutputStreamOperator<ValenciaItem> streamTrafficJam = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, trafficFrequency, collectWithTimestamp, offlineData, skewedDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.TRAFFIC_JAM)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				.process(new ValenciaItemProcessSideOutput(outputTagTraffic)).name(METRIC_VALENCIA_SIDE_OUTPUT)
				;
		SingleOutputStreamOperator<ValenciaItem> streamAirPollution = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, pollutionFrequency, collectWithTimestamp, offlineData, skewedDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.AIR_POLLUTION)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				.process(new ValenciaItemProcessSideOutput(outputTagPollution)).name(METRIC_VALENCIA_SIDE_OUTPUT)
				;

		DataStream<ValenciaItem> streamTrafficJamFiltered;
		DataStream<ValenciaItem> streamAirPollutionFiltered;
		if (enableLookupKeys) {
			// get Side outputs
			DataStream<Tuple2<ValenciaItemType, Long>> sideOutputStreamTraffic = streamTrafficJam.getSideOutput(outputTagTraffic);
			DataStream<Tuple2<ValenciaItemType, Long>> sideOutputStreamPollution = streamAirPollution.getSideOutput(outputTagPollution);
	
			// Lookup keys with Bloom Filter
			streamTrafficJamFiltered = streamTrafficJam
					.connect(sideOutputStreamPollution)
					.keyBy(new ValenciaItemDistrictSelector(), new ValenciaItemLookupKeySelector())
					.process(new ValenciaLookupCoProcess(Time.seconds(60).toMilliseconds())).name(METRIC_VALENCIA_LOOKUP)
					;
			streamAirPollutionFiltered = streamAirPollution
					.connect(sideOutputStreamTraffic)
					.keyBy(new ValenciaItemDistrictSelector(), new ValenciaItemLookupKeySelector())
					.process(new ValenciaLookupCoProcess(Time.seconds(60).toMilliseconds())).name(METRIC_VALENCIA_LOOKUP)
					;
		} else {
			streamTrafficJamFiltered = streamTrafficJam;
			streamAirPollutionFiltered = streamAirPollution;
		}

		// Join -> Print
		streamTrafficJamFiltered
				.join(streamAirPollutionFiltered)
				.where(new ValenciaItemDistrictSelector())
				.equalTo(new ValenciaItemDistrictSelector())
				.window(TumblingEventTimeWindows.of(Time.seconds(60)))
		 		.apply(new TrafficPollutionByDistrictJoinFunction())
		 		.print().name(METRIC_VALENCIA_SINK)
				// .addSink(new MqttStringPublisher(ipAddressSink, topic)).name(METRIC_VALENCIA_SINK)
				;
		//streamTrafficJamFiltered
		//		.keyBy(new ValenciaItemDistrictSelector())
		//		.connect(streamAirPollutionFiltered.keyBy(new ValenciaItemDistrictSelector()))
		//		.process(new ValenciaItemJoinCoProcess(pollutionFrequency, defaultWaterMark)).name(METRIC_VALENCIA_JOIN)
		//		.map(new Valencia2ItemToStringMap()).name(METRIC_VALENCIA_STRING_MAP)
		//		.print().name(METRIC_VALENCIA_SINK)
		//		// .addSink(new MqttStringPublisher(ipAddressSink, topic)).name(METRIC_VALENCIA_SINK)
		//		;

		System.out.println("ExecutionPlan ........................ ");
		System.out.println(env.getExecutionPlan());
		System.out.println("........................ ");
		env.execute(ValenciaDataSkewedBloomFilterJoinExample.class.getName());
		// @formatter:on
	}

	private static class ValenciaItemJoinCoProcess
			extends CoProcessFunction<ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>> {
		private static final long serialVersionUID = -7201692377513092833L;
		private final SimpleDateFormat sdf;
		private final long maxDataSourceFrequency;
		private final long watermarkFrequency;
		private ValueState<String> state;

		public ValenciaItemJoinCoProcess(long maxDataSourceFrequency, long watermarkFrequency) {
			this.sdf = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss");
			this.maxDataSourceFrequency = maxDataSourceFrequency;
			this.watermarkFrequency = watermarkFrequency;
		}

		@Override
		public void processElement1(ValenciaItem traffic,
				CoProcessFunction<ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>>.Context context,
				Collector<Tuple2<ValenciaItem, ValenciaItem>> out) throws Exception {
			long valenciaTimestamp = traffic.getTimestamp();
			long watermark = context.timerService().currentWatermark();
			boolean flag = watermark > 0
					&& (valenciaTimestamp - watermark - watermarkFrequency) < maxDataSourceFrequency;

			String msg = "[" + Thread.currentThread().getId() + " " + traffic.getType() + "] ";
			msg += "ts[" + sdf.format(new Date(valenciaTimestamp)) + "] ";
			msg += "W[" + sdf.format(new Date(watermark)) + "] ";
			msg += "[" + (valenciaTimestamp - watermark - watermarkFrequency) + " " + flag + "] ";
			System.out.println(msg);
			if (flag) {
				state.update(traffic.getType().toString());
				// schedule the next timer 60 seconds from the current event time
				context.timerService().registerEventTimeTimer(watermark + watermarkFrequency);
			}
		}

		@Override
		public void processElement2(ValenciaItem pollution,
				CoProcessFunction<ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>>.Context context,
				Collector<Tuple2<ValenciaItem, ValenciaItem>> out) throws Exception {
		}

		@Override
		public void onTimer(long timestamp,
				CoProcessFunction<ValenciaItem, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>>.OnTimerContext context,
				Collector<Tuple2<ValenciaItem, ValenciaItem>> out) throws Exception {
			long watermark = context.timerService().currentWatermark();
			String msg = "[" + Thread.currentThread().getId() + "] onTimer(" + state.value() + ") ";
			msg += "ts[" + sdf.format(new Date(timestamp)) + "] ";
			msg += "W[" + sdf.format(new Date(watermark)) + "] ";
			msg += "ts[" + timestamp + "] ";
			msg += "W[" + watermark + "] ";
			// System.out.println(msg);
			if (watermark >= timestamp) {

			}
		}
	}

	private void disclaimer() {
		System.out.println("Disclaimer...");
	}
}

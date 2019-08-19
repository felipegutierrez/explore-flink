package org.sense.flink.examples.stream.valencia;

import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_DISTRICT_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_LOOKUP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SIDE_OUTPUT;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SINK;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SOURCE;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.sense.flink.examples.stream.udf.impl.TrafficPollutionByDistrictJoinFunction;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictSelector;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemLookupKeySelector;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemOutputTag;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemProcessSideOutput;
import org.sense.flink.examples.stream.udf.impl.ValenciaLookupCoProcess;
import org.sense.flink.mqtt.MqttStringPublisher;
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

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// @formatter:off
		// Side outputs
		OutputTag<Tuple2<ValenciaItemType, Long>> outputTagTraffic = new ValenciaItemOutputTag("side-output-traffic");
		OutputTag<Tuple2<ValenciaItemType, Long>> outputTagPollution = new ValenciaItemOutputTag("side-output-pollution");

		// Sources -> add synthetic data -> filter
		SingleOutputStreamOperator<ValenciaItem> streamTrafficJam = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, Time.seconds(10).toMilliseconds(), collectWithTimestamp, !offlineData, skewedDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.TRAFFIC_JAM)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				.process(new ValenciaItemProcessSideOutput(outputTagTraffic)).name(METRIC_VALENCIA_SIDE_OUTPUT)
				;
		SingleOutputStreamOperator<ValenciaItem> streamAirPollution = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, Time.seconds(20).toMilliseconds(), collectWithTimestamp, !offlineData, skewedDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.AIR_POLLUTION)
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
					.process(new ValenciaLookupCoProcess(Time.seconds(30).toMilliseconds())).name(METRIC_VALENCIA_LOOKUP)
					;
			streamAirPollutionFiltered = streamAirPollution.keyBy(new ValenciaItemDistrictSelector())
					.connect(sideOutputStreamTraffic)
					.keyBy(new ValenciaItemDistrictSelector(), new ValenciaItemLookupKeySelector())
					.process(new ValenciaLookupCoProcess(Time.seconds(30).toMilliseconds())).name(METRIC_VALENCIA_LOOKUP);
		} else {
			streamTrafficJamFiltered = streamTrafficJam;
			streamAirPollutionFiltered = streamAirPollution;
		}

		// Join -> Print
		streamTrafficJamFiltered
				.join(streamAirPollutionFiltered)
				.where(new ValenciaItemDistrictSelector())
				.equalTo(new ValenciaItemDistrictSelector())
				.window(TumblingEventTimeWindows.of(Time.seconds(40)))
		 		.apply(new TrafficPollutionByDistrictJoinFunction())
		 		// .print().name(METRIC_VALENCIA_SINK)
				.addSink(new MqttStringPublisher(ipAddressSink, topic)).name(METRIC_VALENCIA_SINK)
				;

		System.out.println("ExecutionPlan ........................ ");
		System.out.println(env.getExecutionPlan());
		System.out.println("........................ ");
		env.execute(ValenciaDataSkewedBloomFilterJoinExample.class.getName());
		// @formatter:on
	}

	private void disclaimer() {
		System.out.println("Disclaimer...");
	}
}

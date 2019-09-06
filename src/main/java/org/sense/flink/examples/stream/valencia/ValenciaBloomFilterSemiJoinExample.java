package org.sense.flink.examples.stream.valencia;

import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_DISTRICT_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_LOOKUP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SIDE_OUTPUT;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SINK;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SOURCE;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_WATERMARKER_ASSIGNER;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.sense.flink.examples.stream.udf.impl.TrafficPollutionByDistrictJoinFunction;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemAscendingTimestampExtractor;
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

/**
 * 
 * <pre>
 * This is a good command line to start this application:
 * 
 * ./bin/flink run -c org.sense.flink.App ../app/explore-flink.jar 
 * -app 31 -source 192.168.56.1 -sink 192.168.56.1 -offlineData true -frequencyPull 10 -frequencyWindow 30 -syntheticData true 
 * -optimization true
 * </pre>
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaBloomFilterSemiJoinExample {
	private final String topic = "topic-valencia-semi-join";

	public static void main(String[] args) throws Exception {
		new ValenciaBloomFilterSemiJoinExample("127.0.0.1", "127.0.0.1");
	}

	public ValenciaBloomFilterSemiJoinExample(String ipAddressSource, String ipAddressSink) throws Exception {
		this(ipAddressSource, ipAddressSink, true, 10, 30, true, true);
	}

	public ValenciaBloomFilterSemiJoinExample(String ipAddressSource, String ipAddressSink, boolean offlineData,
			int frequencyPull, int frequencyWindow, boolean skewedDataInjection, boolean optimization)
			throws Exception {
		boolean collectWithTimestamp = false;
		long trafficFrequency = Time.seconds(frequencyPull).toMilliseconds();
		long pollutionFrequency = Time.seconds(frequencyPull).toMilliseconds();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.disableOperatorChaining();

		// @formatter:off
		// Creating side outputs
		OutputTag<Tuple2<ValenciaItemType, Long>> outputTag = new ValenciaItemOutputTag("side-output");

		// Sources -> add synthetic data -> filter
		SingleOutputStreamOperator<ValenciaItem> streamTrafficJam = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, trafficFrequency, collectWithTimestamp, offlineData, skewedDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.TRAFFIC_JAM)
				.assignTimestampsAndWatermarks(new ValenciaItemAscendingTimestampExtractor()).name(METRIC_VALENCIA_WATERMARKER_ASSIGNER)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				;
		SingleOutputStreamOperator<ValenciaItem> streamAirPollution = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, pollutionFrequency, collectWithTimestamp, offlineData, skewedDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.AIR_POLLUTION)
				.assignTimestampsAndWatermarks(new ValenciaItemAscendingTimestampExtractor()).name(METRIC_VALENCIA_WATERMARKER_ASSIGNER)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				.process(new ValenciaItemProcessSideOutput(outputTag)).name(METRIC_VALENCIA_SIDE_OUTPUT)
				;

		// Consuming side outputs
		DataStream<Tuple2<ValenciaItemType, Long>> sideOutputStream = streamAirPollution.getSideOutput(outputTag);

		// Lookup keys with Bloom Filter
		DataStream<ValenciaItem> streamTrafficJamFiltered = streamTrafficJam
				.connect(sideOutputStream)
				.keyBy(new ValenciaItemDistrictSelector(), new ValenciaItemLookupKeySelector())
				.process(new ValenciaLookupCoProcess(Time.seconds(frequencyWindow).toMilliseconds(), optimization)).name(METRIC_VALENCIA_LOOKUP)
				;
		DataStream<ValenciaItem> streamAirPollutionFiltered = streamAirPollution;

		// Join -> Print
		streamTrafficJamFiltered.join(streamAirPollutionFiltered)
				.where(new ValenciaItemDistrictSelector())
				.equalTo(new ValenciaItemDistrictSelector())
				.window(TumblingEventTimeWindows.of(Time.seconds(frequencyWindow)))
		 		.apply(new TrafficPollutionByDistrictJoinFunction())
		 		//.print().name(METRIC_VALENCIA_SINK)
				.addSink(new MqttStringPublisher(ipAddressSink, topic)).name(METRIC_VALENCIA_SINK)
				;

		disclaimer(env.getExecutionPlan(), ipAddressSource);
		env.execute(ValenciaBloomFilterSemiJoinExample.class.getSimpleName());
		// @formatter:on
	}

	private void disclaimer(String logicalPlan, String ipAddressSource) {
		// @formatter:off
		System.out.println("This application (31) aims to show a semi-join operation for stream data which reduces items on the shuffle phase.");
		System.out.println();
		//System.out.println("Changing frequency >>>");
		//System.out.println("It is possible to publish a 'multiply factor' to each item from the source by issuing the commands below.");
		//System.out.println("Each item will be duplicated by 'multiply factor' times.");
		//System.out.println("where: 1 <= 'multiply factor' <=1000");
		//System.out.println("mosquitto_pub -h " + ipAddressSource + " -t " + topicParamFrequencyPull + " -m \"AIR_POLLUTION 500\"");
		//System.out.println("mosquitto_pub -h " + ipAddressSource + " -t " + topicParamFrequencyPull + " -m \"TRAFFIC_JAM 1000\"");
		//System.out.println("mosquitto_pub -h " + ipAddressSource + " -t " + topicParamFrequencyPull + " -m \"NOISE 600\"");
		//System.out.println();
		System.out.println("Use the 'Flink Plan Visualizer' [https://flink.apache.org/visualizer/] in order to see the logical plan of this application.");
		System.out.println("Logical plan >>>");
		System.out.println(logicalPlan);
		System.out.println();
		// @formatter:on
	}
}

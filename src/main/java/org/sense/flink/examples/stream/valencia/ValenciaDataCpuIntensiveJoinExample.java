package org.sense.flink.examples.stream.valencia;

import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_DISTRICT_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_FREQUENCY_PARAMETER;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_FREQUENCY_PARAMETER_SOURCE;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_JOIN;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SINK;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SOURCE;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_STRING_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SYNTHETIC_FLATMAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_WATERMARKER_ASSIGNER;

import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.udf.impl.Valencia2ItemToStringMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemAscendingTimestampExtractor;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictSelector;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemProcessingTimeStdRepartitionJoinCoProcess;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemSyntheticCoFlatMapper;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemTypeParamMap;
import org.sense.flink.mqtt.FlinkMqttConsumer;
import org.sense.flink.mqtt.MqttStringPublisher;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.source.ValenciaItemConsumer;
import org.sense.flink.util.ValenciaItemType;

/**
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaDataCpuIntensiveJoinExample {

	private final String topic = "topic-valencia-data-cpu-intensive";
	private final String topicParamFrequencyPull = "topic-synthetic-frequency-pull";

	public static void main(String[] args) throws Exception {
		new ValenciaDataCpuIntensiveJoinExample("127.0.0.1", "127.0.0.1");
	}

	public ValenciaDataCpuIntensiveJoinExample(String ipAddressSource, String ipAddressSink) throws Exception {
		this(ipAddressSource, ipAddressSink, true, 10, 60);
	}

	public ValenciaDataCpuIntensiveJoinExample(String ipAddressSource, String ipAddressSink, boolean offlineData,
			int frequencyPull, int frequencyWindow) throws Exception {
		boolean collectWithTimestamp = false;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.disableOperatorChaining();

		// @formatter:off
		// Parameter source -> map type and frequency
		DataStream<Tuple2<ValenciaItemType, String>> streamParameter = env
				.addSource(new FlinkMqttConsumer(topicParamFrequencyPull)).name(METRIC_VALENCIA_FREQUENCY_PARAMETER_SOURCE)
				.map(new ValenciaItemTypeParamMap()).name(METRIC_VALENCIA_FREQUENCY_PARAMETER);

		// Sources -> connect (parameter) -> change frequency -> assign timestamp -> map latitude,longitude to district ID
		DataStream<ValenciaItem> streamTrafficJam = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, Time.seconds(frequencyPull).toMilliseconds(), collectWithTimestamp, offlineData)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.TRAFFIC_JAM)
				.connect(streamParameter)
				.keyBy(new NullByteKeySelector<ValenciaItem>(), new NullByteKeySelector<Tuple2<ValenciaItemType, String>>())
				.flatMap(new ValenciaItemSyntheticCoFlatMapper()).name(METRIC_VALENCIA_SYNTHETIC_FLATMAP)
				.assignTimestampsAndWatermarks(new ValenciaItemAscendingTimestampExtractor()).name(METRIC_VALENCIA_WATERMARKER_ASSIGNER)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				;
		DataStream<ValenciaItem> streamAirPollution = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, Time.seconds(frequencyPull).toMilliseconds(), collectWithTimestamp, offlineData)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.AIR_POLLUTION)
				.connect(streamParameter)
				.keyBy(new NullByteKeySelector<ValenciaItem>(), new NullByteKeySelector<Tuple2<ValenciaItemType, String>>())
				.flatMap(new ValenciaItemSyntheticCoFlatMapper()).name(METRIC_VALENCIA_SYNTHETIC_FLATMAP)
				.assignTimestampsAndWatermarks(new ValenciaItemAscendingTimestampExtractor()).name(METRIC_VALENCIA_WATERMARKER_ASSIGNER)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				;

		// Join -> Print
		streamTrafficJam
				.keyBy(new ValenciaItemDistrictSelector())
				.connect(streamAirPollution.keyBy(new ValenciaItemDistrictSelector()))
				.process(new ValenciaItemProcessingTimeStdRepartitionJoinCoProcess(Time.seconds(frequencyWindow).toMilliseconds())).name(METRIC_VALENCIA_JOIN)
				.map(new Valencia2ItemToStringMap()).name(METRIC_VALENCIA_STRING_MAP)
				.addSink(new MqttStringPublisher(ipAddressSink, topic)).name(METRIC_VALENCIA_SINK)
				// .print().name(METRIC_VALENCIA_SINK)
				;		

		disclaimer(env.getExecutionPlan() ,ipAddressSource);
		env.execute(ValenciaDataCpuIntensiveJoinExample.class.getName());
		// @formatter:on
	}

	private void disclaimer(String logicalPlan, String ipAddressSource) {
		// @formatter:off
		System.out.println("This application aims to use intensive CPU.");

		System.out.println("It is possible to publish a 'multiply factor' to each item from the source by issuing the commands below.");
		System.out.println("Each item will be duplicated by 'multiply factor' times.");
		System.out.println("where: 1 <= 'multiply factor' <=200");
		System.out.println("mosquitto_pub -h " + ipAddressSource + " -t " + topicParamFrequencyPull + " -m \"AIR_POLLUTION 500\"");
		System.out.println("mosquitto_pub -h " + ipAddressSource + " -t " + topicParamFrequencyPull + " -m \"TRAFFIC_JAM 1000\"");
		System.out.println("mosquitto_pub -h " + ipAddressSource + " -t " + topicParamFrequencyPull + " -m \"NOISE 600\"");

		System.out.println("Use the 'Flink Plan Visualizer' [https://flink.apache.org/visualizer/] in order to see the logical plan of this application.");
		System.out.println("Logical plan >>>");
		System.out.println(logicalPlan);
		System.out.println();
		// @formatter:on
	}
}

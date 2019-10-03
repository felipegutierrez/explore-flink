package org.sense.flink.examples.stream.valencia;

import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_CPU_INTENSIVE_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_DISTRICT_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SINK;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SOURCE;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_STRING_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_WATERMARKER_ASSIGNER;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.udf.impl.ValenciaIntensiveCpuDistancesMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemAscendingTimestampExtractor;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictSelector;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemEnrichedToStringMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemJoinFunction;
import org.sense.flink.mqtt.MqttStringPublisher;
import org.sense.flink.mqtt.MqttValenciaItemConsumer;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.util.SinkOutputs;
import org.sense.flink.util.ValenciaItemType;

/**
 * <pre>
 * This is a good command line to start this application:
 * 
 * /home/flink/flink-1.9.0/bin/flink run -c org.sense.flink.App /home/flink/explore-flink/target/explore-flink.jar 
 * -app 34 -source 130.239.48.136 -sink 130.239.48.136 -frequencyWindow 30 -parallelism 1 -disableOperatorChaining false -output file
 * </pre>
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaDataMqttCpuIntensiveJoinExample {

	private final String topic = "topic-valencia-cpu-intensive";
	private final String topicParamFrequencyPull = "topic-synthetic-frequency-pull";

	public static void main(String[] args) throws Exception {
		new ValenciaDataMqttCpuIntensiveJoinExample("127.0.0.1", "127.0.0.1");
	}

	public ValenciaDataMqttCpuIntensiveJoinExample(String ipAddressSource, String ipAddressSink) throws Exception {
		this(ipAddressSource, ipAddressSink, 30, 4, false, SinkOutputs.PARAMETER_OUTPUT_FILE);
	}

	public ValenciaDataMqttCpuIntensiveJoinExample(String ipAddressSource, String ipAddressSink, int frequencyWindow,
			int parallelism, boolean disableOperatorChaining, String output) throws Exception {
		boolean collectWithTimestamp = false;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		if (parallelism > 0) {
			env.setParallelism(parallelism);
		}
		if (disableOperatorChaining) {
			env.disableOperatorChaining();
		}

		// @formatter:off
		// Sources -> assign timestamp -> map latitude,longitude to district ID
		DataStream<ValenciaItem> streamTrafficJam = env
				.addSource(new MqttValenciaItemConsumer(ipAddressSource, ValenciaItemType.TRAFFIC_JAM, collectWithTimestamp)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.TRAFFIC_JAM)
				.assignTimestampsAndWatermarks(new ValenciaItemAscendingTimestampExtractor()).name(METRIC_VALENCIA_WATERMARKER_ASSIGNER)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				;
		DataStream<ValenciaItem> streamAirPollution = env
				.addSource(new MqttValenciaItemConsumer(ipAddressSource, ValenciaItemType.AIR_POLLUTION, collectWithTimestamp)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.AIR_POLLUTION)
				.assignTimestampsAndWatermarks(new ValenciaItemAscendingTimestampExtractor()).name(METRIC_VALENCIA_WATERMARKER_ASSIGNER)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				;

		// Join -> intensive CPU task -> map to String -> Publish/Print
		DataStream<String> result = streamTrafficJam.join(streamAirPollution)
				.where(new ValenciaItemDistrictSelector())
				.equalTo(new ValenciaItemDistrictSelector())
				.window(TumblingEventTimeWindows.of(Time.seconds(frequencyWindow)))
				.apply(new ValenciaItemJoinFunction())
				.map(new ValenciaIntensiveCpuDistancesMap()).name(METRIC_VALENCIA_CPU_INTENSIVE_MAP)
				.map(new ValenciaItemEnrichedToStringMap()).name(METRIC_VALENCIA_STRING_MAP)
				;

		if (SinkOutputs.PARAMETER_OUTPUT_FILE.equals(output)) {
			result.print().name(METRIC_VALENCIA_SINK);	
		} else if (SinkOutputs.PARAMETER_OUTPUT_MQTT.equals(output)) {
			result.addSink(new MqttStringPublisher(ipAddressSink, topic)).name(METRIC_VALENCIA_SINK);
		} else {
			result.print().name(METRIC_VALENCIA_SINK);	
		}

		disclaimer(env.getExecutionPlan() ,ipAddressSource);
		env.execute(ValenciaDataMqttCpuIntensiveJoinExample.class.getSimpleName());
		// @formatter:on
	}

	private void disclaimer(String logicalPlan, String ipAddressSource) {
		// @formatter:off
		System.out.println("This is the application [" + ValenciaDataMqttCpuIntensiveJoinExample.class.getSimpleName() + "].");
		System.out.println("It aims to use intensive CPU.");
		System.out.println();
		System.out.println("Use the 'Flink Plan Visualizer' [https://flink.apache.org/visualizer/] in order to see the logical plan of this application.");
		System.out.println("Logical plan >>>");
		System.out.println(logicalPlan);
		System.out.println();
		// @formatter:on
	}
}

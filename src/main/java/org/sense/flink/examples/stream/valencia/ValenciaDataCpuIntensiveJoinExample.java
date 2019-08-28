package org.sense.flink.examples.stream.valencia;

import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_DISTRICT_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_FILTER;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SINK;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SOURCE;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.udf.impl.TrafficPollutionByDistrictJoinFunction;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictSelector;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemFilter;
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

	public static void main(String[] args) throws Exception {
		new ValenciaDataCpuIntensiveJoinExample("127.0.0.1", "127.0.0.1");
	}

	public ValenciaDataCpuIntensiveJoinExample(String ipAddressSource01, String ipAddressSink) throws Exception {
		this(ipAddressSource01, ipAddressSink, true, true, 10, 30);
	}

	public ValenciaDataCpuIntensiveJoinExample(String ipAddressSource01, String ipAddressSink, boolean offlineData,
			boolean syntheticDataInjection, int frequencyPull, int frequencyWindow) throws Exception {
		boolean collectWithTimestamp = true;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// @formatter:off
		// Sources -> add synthetic data -> filter
		DataStream<ValenciaItem> streamTrafficJam = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, Time.seconds(frequencyPull).toMilliseconds(), collectWithTimestamp, offlineData, syntheticDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.TRAFFIC_JAM)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				.filter(new ValenciaItemFilter(ValenciaItemType.TRAFFIC_JAM)).name(METRIC_VALENCIA_FILTER)
				;

		DataStream<ValenciaItem> streamAirPollution = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, Time.seconds(frequencyPull).toMilliseconds(), collectWithTimestamp, offlineData, syntheticDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.AIR_POLLUTION)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				.filter(new ValenciaItemFilter(ValenciaItemType.AIR_POLLUTION)).name(METRIC_VALENCIA_FILTER)
				;

		// Join -> Print
		streamTrafficJam.join(streamAirPollution)
				.where(new ValenciaItemDistrictSelector())
 				.equalTo(new ValenciaItemDistrictSelector())
		 		.window(TumblingEventTimeWindows.of(Time.seconds(frequencyWindow)))
		 		.apply(new TrafficPollutionByDistrictJoinFunction())
		 		.print().name(METRIC_VALENCIA_SINK)
		 		// .addSink(new MqttStringPublisher(ipAddressSink, topic)).name(METRIC_VALENCIA_SINK)
		  		;
		// streamTrafficJam.print();
		// streamAirPollution.print();

		disclaimer(env.getExecutionPlan());
		env.execute(ValenciaDataCpuIntensiveJoinExample.class.getName());
		// @formatter:on
	}

	private void disclaimer(String logicalPlan) {
		System.out.println("This application aims to use intensive CPU.");
		System.out.println(
				"Use the 'Flink Plan Visualizer' [https://flink.apache.org/visualizer/] in order to see the logical plan of this application.");
		System.out.println("Logical plan >>>");
		System.out.println(logicalPlan);
		System.out.println();
	}
}

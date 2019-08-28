package org.sense.flink.examples.stream.valencia;

import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_DISTRICT_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SINK;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SOURCE;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_WATERMARKER_ASSIGNER;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.udf.impl.TrafficPollutionWithDistanceByDistrictJoinFunction;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictSelector;
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
		boolean collectWithTimestamp = false;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// @formatter:off
		// Sources -> map latitude,longitude to district ID
		DataStream<ValenciaItem> streamTrafficJam = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, Time.seconds(frequencyPull).toMilliseconds(), collectWithTimestamp, offlineData, syntheticDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.TRAFFIC_JAM)
				.assignTimestampsAndWatermarks(new MyAscendingTimestampExtractor()).name(METRIC_VALENCIA_WATERMARKER_ASSIGNER)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				;
		DataStream<ValenciaItem> streamAirPollution = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, Time.seconds(frequencyPull).toMilliseconds(), collectWithTimestamp, offlineData, syntheticDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.AIR_POLLUTION)
				.assignTimestampsAndWatermarks(new MyAscendingTimestampExtractor()).name(METRIC_VALENCIA_WATERMARKER_ASSIGNER)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				;

		// Join -> Print
		streamTrafficJam.join(streamAirPollution)
				.where(new ValenciaItemDistrictSelector())
				.equalTo(new ValenciaItemDistrictSelector())
		 		.window(TumblingEventTimeWindows.of(Time.seconds(frequencyWindow)))
		 		.apply(new TrafficPollutionWithDistanceByDistrictJoinFunction())
		 		.print().name(METRIC_VALENCIA_SINK)
		 		// .addSink(new MqttStringPublisher(ipAddressSink, topic)).name(METRIC_VALENCIA_SINK)
		  		;

		disclaimer(env.getExecutionPlan());
		env.execute(ValenciaDataCpuIntensiveJoinExample.class.getName());
		// @formatter:on
	}

	private static class MyAscendingTimestampExtractor extends AscendingTimestampExtractor<ValenciaItem> {
		private static final long serialVersionUID = 4311406052896755965L;

		@Override
		public long extractAscendingTimestamp(ValenciaItem valenciaItem) {
			return valenciaItem.getTimestamp();
		}
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

package org.sense.flink.examples.stream.valencia;

import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_COMBINER;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_DISTRICT_KEY_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_DISTRICT_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SINK;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SOURCE;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_STRING_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_WINDOW;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.operator.impl.MapStreamBundleOperator;
import org.sense.flink.examples.stream.operator.impl.MapStreamBundleOperatorDynamic;
import org.sense.flink.examples.stream.trigger.impl.CountBundleTrigger;
import org.sense.flink.examples.stream.trigger.impl.CountBundleTriggerDynamic;
import org.sense.flink.examples.stream.udf.MapBundleFunction;
import org.sense.flink.examples.stream.udf.impl.MapBundleValenciaImpl;
import org.sense.flink.examples.stream.udf.impl.ValenciaDistrictItemTypeAggWindow;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictAsKeyMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemKeySelector;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemToStringMap;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.source.ValenciaItemConsumer;
import org.sense.flink.util.ValenciaItemType;

/**
 * <pre>
 * This is a good command line to start this application:
 * 
 * ./bin/flink run -c org.sense.flink.App ../app/explore-flink.jar 
 * -app 26 -source 192.168.56.1 -sink 192.168.56.1 -offlineData true -frequencyPull 20 -frequencyWindow 60 -syntheticData true 
 * -optimization true
 * </pre>
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaDataSkewedCombinerExample {

	private final String topic = "topic-valencia-data-combiner";

	public static void main(String[] args) throws Exception {
		new ValenciaDataSkewedCombinerExample("127.0.0.1", "127.0.0.1");
	}

	public ValenciaDataSkewedCombinerExample(String ipAddressSource, String ipAddressSink) throws Exception {
		this(ipAddressSource, ipAddressSink, true, 20, 60, true, false, Long.MAX_VALUE);
	}

	public ValenciaDataSkewedCombinerExample(String ipAddressSource, String ipAddressSink, boolean offlineData,
			int frequencyPull, int frequencyWindow, boolean skewedDataInjection, boolean optimization, long duration)
			throws Exception {
		boolean dynamicCombiner = optimization;
		boolean collectWithTimestamp = true;
		long trafficFrequency = Time.seconds(frequencyPull).toMilliseconds();
		long pollutionFrequency = Time.seconds(frequencyPull).toMilliseconds();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.disableOperatorChaining();

		// @formatter:off
		// Sources -> add synthetic data -> map latitude and longitude to districts in Valencia -> extract the key(district)
		DataStream<Tuple2<Long , ValenciaItem>> streamTrafficJam = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, trafficFrequency, collectWithTimestamp, offlineData, skewedDataInjection, duration)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.TRAFFIC_JAM)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				.map(new ValenciaItemDistrictAsKeyMap()).name(METRIC_VALENCIA_DISTRICT_KEY_MAP)
				;
		DataStream<Tuple2<Long , ValenciaItem>> streamAirPollution = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, pollutionFrequency, collectWithTimestamp, offlineData, skewedDataInjection, duration)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.AIR_POLLUTION)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				.map(new ValenciaItemDistrictAsKeyMap()).name(METRIC_VALENCIA_DISTRICT_KEY_MAP)
				;

		// Union -> Combiner(dynamic or static) -> Average -> Print
		streamTrafficJam.union(streamAirPollution)
				.transform(METRIC_VALENCIA_COMBINER, TypeInformation.of(new TypeHint<Tuple2<Long, ValenciaItem>>(){}), getCombinerOperator(dynamicCombiner)).name(METRIC_VALENCIA_COMBINER)
				.keyBy(new ValenciaItemKeySelector())
				.window(TumblingProcessingTimeWindows.of(Time.seconds(frequencyWindow)))
				.apply(new ValenciaDistrictItemTypeAggWindow()).name(METRIC_VALENCIA_WINDOW)
				.map(new ValenciaItemToStringMap()).name(METRIC_VALENCIA_STRING_MAP)
				//.addSink(new MqttStringPublisher(ipAddressSink, topic)).name(METRIC_VALENCIA_SINK)
				.print().name(METRIC_VALENCIA_SINK)
				;

		disclaimer(env.getExecutionPlan(), ipAddressSource);
		env.execute(ValenciaDataSkewedCombinerExample.class.getSimpleName());
		// @formatter:on
	}

	private OneInputStreamOperator<Tuple2<Long, ValenciaItem>, Tuple2<Long, ValenciaItem>> getCombinerOperator(
			boolean dynamicCombiner) throws Exception {
		// @formatter:off
		MapBundleFunction<Long, ValenciaItem, Tuple2<Long, ValenciaItem>, Tuple2<Long, ValenciaItem>> myMapBundleFunction = new MapBundleValenciaImpl();
		KeySelector<Tuple2<Long, ValenciaItem>, Long> keyBundleSelector = (KeySelector<Tuple2<Long, ValenciaItem>, Long>) value -> value.f0;

		if (dynamicCombiner) {
			CountBundleTriggerDynamic<Long, Tuple2<Long, ValenciaItem>> bundleTrigger = new CountBundleTriggerDynamic<Long, Tuple2<Long, ValenciaItem>>();
			return new MapStreamBundleOperatorDynamic<Long, ValenciaItem, Tuple2<Long, ValenciaItem>, Tuple2<Long, ValenciaItem>>(myMapBundleFunction, bundleTrigger, keyBundleSelector);
		} else {
			long tupleAmountToCombine = 10;
			CountBundleTrigger<Tuple2<Long, ValenciaItem>> bundleTrigger = new CountBundleTrigger<Tuple2<Long, ValenciaItem>>(tupleAmountToCombine);
			return new MapStreamBundleOperator<>(myMapBundleFunction, bundleTrigger, keyBundleSelector);
		}
		// @formatter:on
	}

	private void disclaimer(String logicalPlan, String ipAddressSource) {
		// @formatter:off
		System.out.println("This is the application [" + ValenciaDataSkewedCombinerExample.class.getSimpleName() + "].");
		System.out.println("It aims to show a dynamic combiner operation for stream data which reduces items on the shuffle phase.");
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

package org.sense.flink.examples.stream.valencia;

import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_DISTRICT_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_LOOKUP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SIDE_OUTPUT;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SINK;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SOURCE;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.sense.flink.examples.stream.udf.impl.TrafficPollutionByDistrictJoinFunction;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictSelector;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.source.ValenciaItemConsumer;
import org.sense.flink.util.ValenciaItemType;

import com.clearspring.analytics.stream.membership.BloomFilter;
import com.clearspring.analytics.stream.membership.Filter;

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

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// @formatter:off
		// Side outputs
		OutputTag<Tuple2<ValenciaItemType, Long>> outputTagTraffic = new ValenciaItemOutputTag("side-output-traffic");
		OutputTag<Tuple2<ValenciaItemType, Long>> outputTagPollution = new ValenciaItemOutputTag("side-output-pollution");

		// Sources -> add synthetic data -> filter
		SingleOutputStreamOperator<ValenciaItem> streamTrafficJam = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, Time.seconds(20).toMilliseconds(), collectWithTimestamp, !offlineData, skewedDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.TRAFFIC_JAM)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				.process(new ValenciaItemProcessSideOutput(outputTagTraffic)).name(METRIC_VALENCIA_SIDE_OUTPUT)
				;
		SingleOutputStreamOperator<ValenciaItem> streamAirPollution = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, Time.seconds(60).toMilliseconds(), collectWithTimestamp, !offlineData, skewedDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.AIR_POLLUTION)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				.process(new ValenciaItemProcessSideOutput(outputTagPollution)).name(METRIC_VALENCIA_SIDE_OUTPUT)
				;

		// get Side outputs
		DataStream<Tuple2<ValenciaItemType, Long>> sideOutputStreamTraffic = streamTrafficJam.getSideOutput(outputTagTraffic);
		DataStream<Tuple2<ValenciaItemType, Long>> sideOutputStreamPollution = streamAirPollution.getSideOutput(outputTagPollution);

		// Lookup keys with Bloom Filter
		DataStream<ValenciaItem> streamTrafficJamFiltered = streamTrafficJam//.name(METRIC_VALENCIA_SIDE_OUTPUT)
				.connect(sideOutputStreamPollution)
				.flatMap(new ValenciaLookupCoFlatMap()).name(METRIC_VALENCIA_LOOKUP);
		DataStream<ValenciaItem> streamAirPollutionFiltered = streamAirPollution//.name(METRIC_VALENCIA_SIDE_OUTPUT)
				.connect(sideOutputStreamTraffic)
				.flatMap(new ValenciaLookupCoFlatMap()).name(METRIC_VALENCIA_LOOKUP);

		// Join -> Print
		streamTrafficJamFiltered
				.join(streamAirPollutionFiltered)
				.where(new ValenciaItemDistrictSelector())
				.equalTo(new ValenciaItemDistrictSelector())
				.window(TumblingEventTimeWindows.of(Time.seconds(20)))
		 		.apply(new TrafficPollutionByDistrictJoinFunction())
		 		.print().name(METRIC_VALENCIA_SINK)
				// .addSink(new MqttStringPublisher(ipAddressSink, topic)).name(METRIC_VALENCIA_SINK)
				;

		System.out.println("ExecutionPlan ........................ ");
		System.out.println(env.getExecutionPlan());
		System.out.println("........................ ");
		env.execute(ValenciaDataSkewedBloomFilterJoinExample.class.getName());
		// @formatter:on
	}

	private static class ValenciaItemOutputTag extends OutputTag<Tuple2<ValenciaItemType, Long>> {
		private static final long serialVersionUID = 1024854737660323566L;

		public ValenciaItemOutputTag(String id) {
			super(id);
		}
	}

	private static class ValenciaItemProcessSideOutput extends ProcessFunction<ValenciaItem, ValenciaItem> {
		private static final long serialVersionUID = -8186522079661070009L;
		private OutputTag<Tuple2<ValenciaItemType, Long>> outputTag;

		public ValenciaItemProcessSideOutput(OutputTag<Tuple2<ValenciaItemType, Long>> outputTag) {
			this.outputTag = outputTag;
		}

		@Override
		public void processElement(ValenciaItem valenciaItem, ProcessFunction<ValenciaItem, ValenciaItem>.Context ctx,
				Collector<ValenciaItem> out) throws Exception {
			out.collect(valenciaItem);
			ctx.output(outputTag, Tuple2.of(valenciaItem.getType(), valenciaItem.getId()));
		}
	}

	/**
	 * The idea of this UDF is to send tuples that have a possibility to be in the
	 * join operation on the following operator. We will send tuples only that
	 * already match with items of the Side Output source.
	 * 
	 * @author Felipe Oliveira Gutierrez
	 */
	private static class ValenciaLookupCoFlatMap
			extends RichCoFlatMapFunction<ValenciaItem, Tuple2<ValenciaItemType, Long>, ValenciaItem> {
		private static final long serialVersionUID = -5653918629637391518L;
		private Filter bloomFilterTrafficMatches;
		private Filter bloomFilterPollutionMatches;
		private Filter bloomFilterTrafficRedundant;
		private Filter bloomFilterPollutionRedundant;

		@Override
		public void open(Configuration parameters) throws Exception {
			bloomFilterTrafficMatches = new BloomFilter(100, 0.01);
			bloomFilterPollutionMatches = new BloomFilter(100, 0.01);
			bloomFilterTrafficRedundant = new BloomFilter(100, 0.01);
			bloomFilterPollutionRedundant = new BloomFilter(100, 0.01);
		}

		/**
		 * The key is it will give false positive result, but never false negative. If
		 * the answer is TRUE it is not accurate. If the answer is FALSE it is within
		 * 100% accuracy.
		 */
		@Override
		public void flatMap1(ValenciaItem value, Collector<ValenciaItem> out) throws Exception {
			String key = value.getId().toString();
			if (value.getType() == ValenciaItemType.TRAFFIC_JAM) {
				// If the key is not redundant and if it is likely to match
				if (!bloomFilterTrafficRedundant.isPresent(key) && bloomFilterPollutionMatches.isPresent(key)) {
					out.collect(value);
					bloomFilterTrafficRedundant.add(key);
				}
			} else if (value.getType() == ValenciaItemType.AIR_POLLUTION) {
				// If the key is not redundant and if it is likely to match
				if (!bloomFilterPollutionRedundant.isPresent(key) && bloomFilterTrafficMatches.isPresent(key)) {
					out.collect(value);
					bloomFilterPollutionRedundant.add(key);
				}
			} else if (value.getType() == ValenciaItemType.NOISE) {
			} else {
				throw new Exception("ValenciaItemType is NULL!");
			}
		}

		@Override
		public void flatMap2(Tuple2<ValenciaItemType, Long> lookupValue, Collector<ValenciaItem> out) throws Exception {
			String key = lookupValue.f1.toString();
			if (lookupValue.f0 == ValenciaItemType.TRAFFIC_JAM) {
				bloomFilterTrafficMatches.add(key);
			} else if (lookupValue.f0 == ValenciaItemType.AIR_POLLUTION) {
				bloomFilterPollutionMatches.add(key);
			} else if (lookupValue.f0 == ValenciaItemType.NOISE) {
			} else {
				throw new Exception("ValenciaItemType is NULL!");
			}
		}
	}

	private void disclaimer() {
		System.out.println("Disclaimer...");
	}
}

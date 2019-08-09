package org.sense.flink.examples.stream;

import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SINK;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_STRING_MAP;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictSelector;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemFilter;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemSyntheticData;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemToStringMap;
import org.sense.flink.mqtt.MqttStringPublisher;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.pojo.ValenciaItemAvg;
import org.sense.flink.source.ValenciaItemConsumer;
import org.sense.flink.util.ValenciaItemType;

/**
 * 
 * based on
 * 
 * https://github.com/ververica/flink-training-exercises/blob/master/src/main/java/com/dataartisans/flinktraining/exercises/datastream_java/process/ExpiringStateExercise.java
 * https://github.com/ververica/flink-training-exercises/blob/master/src/main/java/com/dataartisans/flinktraining/exercises/datastream_java/process/ProcessingTimeJoinExercise.java
 * https://github.com/ververica/flink-training-exercises/blob/master/src/main/java/com/dataartisans/flinktraining/exercises/datastream_java/process/EventTimeJoinExercise.java
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaDataSkewedMyIntevalJoinExample {

	private final String topic = "topic-valencia-join-data-skewed";

	public static void main(String[] args) throws Exception {
		new ValenciaDataSkewedMyIntevalJoinExample("127.0.0.1", "127.0.0.1");
	}

	public ValenciaDataSkewedMyIntevalJoinExample(String ipAddressSource01, String ipAddressSink) throws Exception {
		disclaimer();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// @formatter:off
		// Static coordinate to create synthetic data
		// Point point = new Point(725140.37, 4371855.492); // id=3, district=Extramurs
		Point point = new Point(726777.707, 4369824.436); // id=10, district=Quatre Carreres
		double distance = 1000.0; // distance in meters
		long districtId = 10;

		// Sources -> add synthetic data -> filter
		DataStream<ValenciaItem> streamTrafficJam = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, Time.minutes(5).toMilliseconds(), true)).name(ValenciaItemConsumer.class.getName())
				.map(new ValenciaItemDistrictMap()).name(ValenciaItemDistrictMap.class.getName())
				.flatMap(new ValenciaItemSyntheticData(ValenciaItemType.TRAFFIC_JAM, point, distance)).name(ValenciaItemSyntheticData.class.getName())
				.filter(new ValenciaItemFilter(ValenciaItemType.TRAFFIC_JAM)).name(ValenciaItemFilter.class.getName())
				;

		DataStream<ValenciaItem> streamAirPollution = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, Time.minutes(30).toMilliseconds(), true)).name(ValenciaItemConsumer.class.getName())
				.map(new ValenciaItemDistrictMap()).name(ValenciaItemDistrictMap.class.getName())
				.flatMap(new ValenciaItemSyntheticData(ValenciaItemType.AIR_POLLUTION, point, distance, districtId)).name(ValenciaItemSyntheticData.class.getName())
				.filter(new ValenciaItemFilter(ValenciaItemType.AIR_POLLUTION)).name(ValenciaItemFilter.class.getName())
				;

		streamTrafficJam
				.keyBy(new ValenciaItemDistrictSelector())
				.connect(streamAirPollution.keyBy(new ValenciaItemDistrictSelector()))
				.process(new EventTimeCoProcessFunction())
				.map(new ValenciaItemToStringMap()).name(METRIC_VALENCIA_STRING_MAP)
				.addSink(new MqttStringPublisher(ipAddressSink, topic)).name(METRIC_VALENCIA_SINK)
				// .print()
				;

		// streamTrafficJam.print();
		// streamAirPollution.print();

		env.execute(ValenciaDataSkewedMyIntevalJoinExample.class.getName());
		// @formatter:on
	}

	private void disclaimer() {
		System.out.println("Disclaimer...");
	}

	public static class EventTimeCoProcessFunction
			extends CoProcessFunction<ValenciaItem, ValenciaItem, Tuple2<Long, ValenciaItem>> {
		private static final long serialVersionUID = -2310556357800284550L;
		// Store pending Traffic and Pollution for a districtId, keyed by timestamp
		private MapState<Long, ValenciaItem> trafficMap = null;
		private MapState<Long, ValenciaItem> pollutionMap = null;

		@Override
		public void open(Configuration config) {
			MapStateDescriptor<Long, ValenciaItem> trafficDescriptor = new MapStateDescriptor<Long, ValenciaItem>(
					"trafficBuffer", TypeInformation.of(Long.class), TypeInformation.of(ValenciaItem.class));
			trafficMap = getRuntimeContext().getMapState(trafficDescriptor);

			MapStateDescriptor<Long, ValenciaItem> pollutionDescriptor = new MapStateDescriptor<Long, ValenciaItem>(
					"pollutionBuffer", TypeInformation.of(Long.class), TypeInformation.of(ValenciaItem.class));
			pollutionMap = getRuntimeContext().getMapState(pollutionDescriptor);
		}

		@Override
		public void processElement1(ValenciaItem traffic,
				CoProcessFunction<ValenciaItem, ValenciaItem, Tuple2<Long, ValenciaItem>>.Context context,
				Collector<Tuple2<Long, ValenciaItem>> out) throws Exception {
			System.out.println("Received " + traffic);
			TimerService timerService = context.timerService();

			if (context.timestamp() > timerService.currentWatermark()) {
				// Do the join later, by which time any relevant Pollution records should have
				// arrived
				trafficMap.put(traffic.getTimestamp(), traffic);
				timerService.registerEventTimeTimer(traffic.getTimestamp());
			} else {
				// Late Traffic records land here.
				System.out.println("Late traffic record");
			}
		}

		@Override
		public void processElement2(ValenciaItem pollution,
				CoProcessFunction<ValenciaItem, ValenciaItem, Tuple2<Long, ValenciaItem>>.Context context,
				Collector<Tuple2<Long, ValenciaItem>> out) throws Exception {
			System.out.println("Received " + pollution.toString());
			pollutionMap.put(pollution.getTimestamp(), pollution);

			/*
			 * Calling this solely for its side effect of freeing older Customer records.
			 * Otherwise Customers with frequent updates and no Trades would leak state.
			 */
			getPollutionRecordToJoin(context.timerService().currentWatermark());
		}

		@Override
		public void onTimer(long t, OnTimerContext context, Collector<Tuple2<Long, ValenciaItem>> out)
				throws Exception {
			ValenciaItem traffic = trafficMap.get(t);
			if (traffic != null) {
				trafficMap.remove(t);

				ValenciaItem pollution = getPollutionRecordToJoin(traffic.getTimestamp());
				String value = traffic.getValue() + " - " + pollution.getValue();

				ValenciaItem joined = new ValenciaItemAvg(traffic.getId(), traffic.getAdminLevel(),
						traffic.getDistrict(), traffic.getUpdate(), null, value);
				out.collect(Tuple2.of(2L, joined));
			}
		}

		/*
		 * Returns the newest Pollution that isn't newer than the Traffic we are
		 * enriching. As a side effect, removes earlier Pollution records that are no
		 * longer needed.
		 */
		private ValenciaItem getPollutionRecordToJoin(Long timestamp) throws Exception {
			Iterator<Map.Entry<Long, ValenciaItem>> pollutionEntries = pollutionMap.iterator();
			ValenciaItem theOneWeAreLookingFor = null;
			List<Long> toRemove = new ArrayList<Long>();

			while (pollutionEntries.hasNext()) {
				ValenciaItem c = pollutionEntries.next().getValue();
				// c should not be newer than the Trade being enriched
				if (c.getTimestamp() <= timestamp) {
					/*
					 * By the time Trades are being joined, they are being processed in order (by
					 * Timestamp). This means that any Customer record too old to join with this
					 * Trade is too old to be worth keeping any longer.
					 */
					if (theOneWeAreLookingFor != null) {
						if (c.getTimestamp() > theOneWeAreLookingFor.getTimestamp()) {
							toRemove.add(theOneWeAreLookingFor.getTimestamp());
							theOneWeAreLookingFor = c;
						}
					} else {
						theOneWeAreLookingFor = c;
					}
				}
			}
			for (Long t : toRemove) {
				System.out.println("Removing pollution @ " + t);
				pollutionMap.remove(t);
			}
			return theOneWeAreLookingFor;
		}
	}
}

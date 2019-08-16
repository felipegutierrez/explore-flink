package org.sense.flink.examples.stream.valencia;

import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_DISTRICT_MAP;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SINK;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SOURCE;
import static org.sense.flink.util.MetricLabels.METRIC_VALENCIA_SYNTHETIC_FLATMAP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.sense.flink.examples.stream.udf.CleanupState;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemSyntheticData;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.source.ValenciaItemConsumer;
import org.sense.flink.util.CRSCoordinateTransformer;
import org.sense.flink.util.ValenciaItemType;

public class ValenciaDataSkewedCustomWindowExample {
	private final String topic = "topic-valencia-data-skewed";

	public static void main(String[] args) throws Exception {
		new ValenciaDataSkewedCustomWindowExample("127.0.0.1", "127.0.0.1");
	}

	public ValenciaDataSkewedCustomWindowExample(String ipAddressSource01, String ipAddressSink) throws Exception {
		disclaimer();
		List<Tuple4<Point, Long, Long, String>> coordinates = syntheticCoordinates();
		boolean offlineData = true;
		boolean collectWithTimestamp = true;
		boolean skewedDataInjection = true;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// @formatter:off
		// Sources -> add synthetic data -> filter
		DataStream<ValenciaItem> streamTrafficJam = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, Time.seconds(20).toMilliseconds(), collectWithTimestamp, !offlineData, skewedDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.TRAFFIC_JAM)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				.flatMap(new ValenciaItemSyntheticData(ValenciaItemType.TRAFFIC_JAM, coordinates)).name(METRIC_VALENCIA_SYNTHETIC_FLATMAP)
				;

		DataStream<ValenciaItem> streamAirPollution = env
				.addSource(new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, Time.seconds(60).toMilliseconds(), collectWithTimestamp, !offlineData, skewedDataInjection)).name(METRIC_VALENCIA_SOURCE + "-" + ValenciaItemType.AIR_POLLUTION)
				.map(new ValenciaItemDistrictMap()).name(METRIC_VALENCIA_DISTRICT_MAP)
				.flatMap(new ValenciaItemSyntheticData(ValenciaItemType.AIR_POLLUTION, coordinates)).name(METRIC_VALENCIA_SYNTHETIC_FLATMAP)
				;

		// Join -> Print
		streamTrafficJam
			.union(streamAirPollution)
			.keyBy(new ValenciaItemDistrictSelectorBloomFilter())
			// .process(new KeyedProcessFunctionWithCleanupState(2, 10))
			// .join(streamAirPollution)
			// .where(new ValenciaItemDistrictSelector())
			// .equalTo(new ValenciaItemDistrictSelector())
			// .window(MyTumblingEventTimeWindows.of(Time.seconds(20)))
			// .process(function)
			// .apply(new TrafficPollutionByDistrictJoinFunction())
			.print().name(METRIC_VALENCIA_SINK)
			// .addSink(new MqttStringPublisher(ipAddressSink, topic)).name(METRIC_VALENCIA_SINK)
			;
		// streamTrafficJam.print();
		// streamAirPollution.print();

		System.out.println("ExecutionPlan ........................ ");
		System.out.println(env.getExecutionPlan());
		System.out.println("........................ ");
		env.execute(ValenciaDataSkewedCustomWindowExample.class.getName());
		// @formatter:on
	}

	private static class ValenciaItemDistrictSelectorBloomFilter implements KeySelector<ValenciaItem, Long> {
		private static final long serialVersionUID = -5690033101189244233L;

		@Override
		public Long getKey(ValenciaItem value) throws Exception {
			return value.getId();
		}
	}

	private static class KeyedProcessFunctionWithCleanupState extends
			KeyedProcessFunction<Long, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>> implements CleanupState {

		private final long minRetentionTime;
		private final long maxRetentionTime;
		protected final boolean stateCleaningEnabled;

		// holds the latest registered cleanup timer
		private ValueState<Long> cleanupTimeState;

		public KeyedProcessFunctionWithCleanupState(long minRetentionTime, long maxRetentionTime) {
			this.minRetentionTime = minRetentionTime;
			this.maxRetentionTime = maxRetentionTime;
			this.stateCleaningEnabled = minRetentionTime > 1;
		}

		protected void initCleanupTimeState(String stateName) {
			if (stateCleaningEnabled) {
				ValueStateDescriptor<Long> inputCntDescriptor = new ValueStateDescriptor<>(stateName, Types.LONG);
				cleanupTimeState = getRuntimeContext().getState(inputCntDescriptor);
			}
		}

		protected void registerProcessingCleanupTimer(Context ctx, long currentTime) throws Exception {
			if (stateCleaningEnabled) {
				registerProcessingCleanupTimer(cleanupTimeState, currentTime, minRetentionTime, maxRetentionTime,
						ctx.timerService());
			}
		}

		protected boolean isProcessingTimeTimer(OnTimerContext ctx) {
			return ctx.timeDomain() == TimeDomain.PROCESSING_TIME;
		}

		protected void cleanupState(State... states) {
			for (State state : states) {
				state.clear();
			}
			this.cleanupTimeState.clear();
		}

		protected Boolean needToCleanupState(Long timestamp) throws IOException {
			if (stateCleaningEnabled) {
				Long cleanupTime = cleanupTimeState.value();
				// check that the triggered timer is the last registered processing time timer.
				return timestamp.equals(cleanupTime);
			} else {
				return false;
			}
		}

		@Override
		public void processElement(ValenciaItem arg0,
				KeyedProcessFunction<Long, ValenciaItem, Tuple2<ValenciaItem, ValenciaItem>>.Context arg1,
				Collector<Tuple2<ValenciaItem, ValenciaItem>> arg2) throws Exception {
		}
	}

	private static class MyTumblingEventTimeWindows extends WindowAssigner<Object, TimeWindow> {
		private static final long serialVersionUID = 1L;
		private final long size;
		private final long offset;

		protected MyTumblingEventTimeWindows(long size, long offset) {
			if (Math.abs(offset) >= size) {
				throw new IllegalArgumentException(
						"TumblingEventTimeWindows parameters must satisfy abs(offset) < size");
			}
			this.size = size;
			this.offset = offset;
		}

		@Override
		public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
			if (timestamp > Long.MIN_VALUE) {
				// Long.MIN_VALUE is currently assigned when no timestamp is present
				long start = TimeWindow.getWindowStartWithOffset(timestamp, offset, size);
				return Collections.singletonList(new TimeWindow(start, start + size));
			} else {
				throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). "
						+ "Is the time characteristic set to 'ProcessingTime', or did you forget to call "
						+ "'DataStream.assignTimestampsAndWatermarks(...)'?");
			}
		}

		@Override
		public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
			return EventTimeTrigger.create();
		}

		@Override
		public String toString() {
			return "TumblingEventTimeWindows(" + size + ")";
		}

		/**
		 * Creates a new {@code TumblingEventTimeWindows} {@link WindowAssigner} that
		 * assigns elements to time windows based on the element timestamp.
		 *
		 * @param size
		 *            The size of the generated windows.
		 * @return The time policy.
		 */
		public static MyTumblingEventTimeWindows of(Time size) {
			return new MyTumblingEventTimeWindows(size.toMilliseconds(), 0);
		}

		/**
		 * Creates a new {@code TumblingEventTimeWindows} {@link WindowAssigner} that
		 * assigns elements to time windows based on the element timestamp and offset.
		 *
		 * <p>
		 * For example, if you want window a stream by hour,but window begins at the
		 * 15th minutes of each hour, you can use
		 * {@code of(Time.hours(1),Time.minutes(15))},then you will get time windows
		 * start at 0:15:00,1:15:00,2:15:00,etc.
		 *
		 * <p>
		 * Rather than that,if you are living in somewhere which is not using UTC±00:00
		 * time, such as China which is using UTC+08:00,and you want a time window with
		 * size of one day, and window begins at every 00:00:00 of local time,you may
		 * use {@code of(Time.days(1),Time.hours(-8))}. The parameter of offset is
		 * {@code Time.hours(-8))} since UTC+08:00 is 8 hours earlier than UTC time.
		 *
		 * @param size
		 *            The size of the generated windows.
		 * @param offset
		 *            The offset which window start would be shifted by.
		 * @return The time policy.
		 */
		public static MyTumblingEventTimeWindows of(Time size, Time offset) {
			return new MyTumblingEventTimeWindows(size.toMilliseconds(), offset.toMilliseconds());
		}

		@Override
		public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
			return new TimeWindow.Serializer();
		}

		@Override
		public boolean isEventTime() {
			return true;
		}
	}

	private List<Tuple4<Point, Long, Long, String>> syntheticCoordinates() {
		// Static coordinate to create synthetic data
		List<Tuple4<Point, Long, Long, String>> coordinates = new ArrayList<Tuple4<Point, Long, Long, String>>();
		coordinates.add(Tuple4.of(new Point(725140.37, 4371855.492, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE), 3L,
				9L, "Extramurs"));
		coordinates.add(Tuple4.of(new Point(726777.707, 4369824.436, CRSCoordinateTransformer.DEFAULT_CRS_SOURCE), 10L,
				9L, "Quatre Carreres"));
		return coordinates;
	}

	private void disclaimer() {
		System.out.println("Disclaimer...");
	}
}

package org.sense.flink.examples.stream;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

/**
 * On the terminal execute "nc -lk 9000", run this class and type words back on
 * the terminal
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class WordHLLKeyedProcessWindowTwitter {

	public static void main(String[] args) throws Exception {
		new WordHLLKeyedProcessWindowTwitter();
	}

	public WordHLLKeyedProcessWindowTwitter() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.getConfig().disableSysoutLogging();
		// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		// @formatter:off
		Properties props = new Properties();
		props.setProperty(TwitterSource.CONSUMER_KEY, "******************");
		props.setProperty(TwitterSource.CONSUMER_SECRET, "******************");
		props.setProperty(TwitterSource.TOKEN, "******************");
		props.setProperty(TwitterSource.TOKEN_SECRET, "******************");
		DataStream<String> streamSource = env.addSource(new TwitterSource(props));

		DataStream<Tuple3<Integer, Long, String>> tweets = streamSource
				.flatMap(new SelectWordsAndTokenizeFlatMap())
				.keyBy(new WordKeySelector())
				.process(new HLLKeyedProcessFunction());

		tweets.print();
		// @formatter:on

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("WordHLLKeyedProcessWindowTwitter");
	}

	public static class SelectWordsAndTokenizeFlatMap implements FlatMapFunction<String, Tuple2<Integer, String>> {
		private static final long serialVersionUID = -7130548585178892323L;
		private transient ObjectMapper jsonParser;

		/**
		 * Select the language from the incoming JSON text.
		 */
		@Override
		public void flatMap(String value, Collector<Tuple2<Integer, String>> out) throws Exception {
			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}
			JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
			// boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang")
			// && jsonNode.get("user").get("lang").asText().equals("en");
			boolean isEnglish = jsonNode.has("lang") && jsonNode.get("lang").asText().equalsIgnoreCase("en");
			boolean isPortuguese = jsonNode.has("lang") && jsonNode.get("lang").asText().equalsIgnoreCase("pt");
			boolean isSpanish = jsonNode.has("lang") && jsonNode.get("lang").asText().equalsIgnoreCase("es");
			boolean isGerman = jsonNode.has("lang") && jsonNode.get("lang").asText().equalsIgnoreCase("de");
			boolean hasText = jsonNode.has("text");
			if ((isEnglish || isPortuguese || isSpanish || isGerman) && hasText) {
				// message of tweet
				StringTokenizer tokenizer = new StringTokenizer(jsonNode.get("text").asText());

				// split the message
				while (tokenizer.hasMoreTokens()) {
					String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();
					if (!result.isEmpty()) {
						out.collect(new Tuple2<Integer, String>(0, result));
					}
				}
			}
		}
	}

	public static class WordKeySelector implements KeySelector<Tuple2<Integer, String>, Integer> {
		private static final long serialVersionUID = 6592395228485429716L;

		@Override
		public Integer getKey(Tuple2<Integer, String> value) throws Exception {
			return value.f0;
		}
	}

	public static class HLLWithTimestamp implements Serializable {
		private static final long serialVersionUID = -2595988668847108995L;
		// public HyperLogLog hyperLogLog;
		public Set<String> distinctWords;
		public long lastModified;

		public HLLWithTimestamp() {
			// this.hyperLogLog = new HyperLogLog(16);
			this.distinctWords = new HashSet<String>();
			this.lastModified = 0L;
		}

		// public boolean offer(Object value) {
		// return hyperLogLog.offer(value);
		// }
		// public long cardinality() {
		// return hyperLogLog.cardinality();
		// }
	}

	public static class HLLKeyedProcessFunction
			extends KeyedProcessFunction<Integer, Tuple2<Integer, String>, Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = -1846420848264179650L;
		private ValueState<HLLWithTimestamp> state;

		@Override
		public void open(Configuration parameters) throws Exception {
			state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", HLLWithTimestamp.class));
		}

		@Override
		public void processElement(Tuple2<Integer, String> value,
				KeyedProcessFunction<Integer, Tuple2<Integer, String>, Tuple3<Integer, Long, String>>.Context ctx,
				Collector<Tuple3<Integer, Long, String>> out) throws Exception {
			Integer key = value.f0;
			String word = value.f1;
			// retrieve the current state
			HLLWithTimestamp current = state.value();
			if (current == null) {
				current = new HLLWithTimestamp();
			}
			// add a value on the HLL sketch
			current.distinctWords.add(word);
			// current.offer(word);

			// set the state's timestamp to the record's assigned event time timestamp
			current.lastModified = ctx.timestamp();

			// write the state back
			state.update(current);

			// schedule the next timer 5 seconds from the current event time
			ctx.timerService().registerEventTimeTimer(current.lastModified + 5000);
		}

		@Override
		public void onTimer(long timestamp,
				KeyedProcessFunction<Integer, Tuple2<Integer, String>, Tuple3<Integer, Long, String>>.OnTimerContext ctx,
				Collector<Tuple3<Integer, Long, String>> out) throws Exception {
			// get the state for the key that scheduled the timer
			HLLWithTimestamp result = state.value();

			// check if this is an outdated timer or the latest timer
			System.out.println("onTimer: " + timestamp + " - " + (long) result.lastModified);
			System.out.println("onTimer: " + timestamp + " - " + ((long) (result.lastModified + 5000)));
			if (timestamp == (long) (result.lastModified + 5000)) {
				String distincWords = "";
				for (String word : result.distinctWords) {
					distincWords = distincWords + word + "-";
				}
				int distinctCount = result.distinctWords.size();
				// double err = Math.abs(result.cardinality() - distinctCount) / (double)
				// distinctCount;
				System.out.println("-------------------------");
				System.out.println("exact cardinality    : " + distinctCount);
				// System.out.println("estimated cardinality: " + result.cardinality());
				// System.out.println("Error : " + err);
				// emit the state on timeout
				// out.collect(new Tuple3<Integer, Long, String>(distinctCount,
				// result.cardinality(), distincWords));
				out.collect(new Tuple3<Integer, Long, String>(distinctCount, 0L, distincWords));
			}
		}
	}
}

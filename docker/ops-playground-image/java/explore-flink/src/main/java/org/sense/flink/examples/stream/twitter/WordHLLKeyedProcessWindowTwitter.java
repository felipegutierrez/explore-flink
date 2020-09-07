package org.sense.flink.examples.stream.twitter;

import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.sense.flink.util.HyperLogLogState;

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

		// @formatter:off
		Properties props = new Properties();
		props.setProperty(TwitterSource.CONSUMER_KEY, "******************");
		props.setProperty(TwitterSource.CONSUMER_SECRET, "******************");
		props.setProperty(TwitterSource.TOKEN, "******************");
		props.setProperty(TwitterSource.TOKEN_SECRET, "******************");
		DataStream<String> streamSource = env.addSource(new TwitterSource(props));

		streamSource
				.flatMap(new SelectWordsAndTokenizeFlatMap())
				.keyBy(new NullByteKeySelector<String>())
				.process(new TimeOutDistinctAndCountFunction(5 * 1000))
				.print();
		// @formatter:on

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("WordHLLKeyedProcessWindowTwitter");
	}

	public static class SelectWordsAndTokenizeFlatMap implements FlatMapFunction<String, String> {
		private static final long serialVersionUID = -5963474968319958217L;
		private transient ObjectMapper jsonParser;

		/**
		 * Select the language from the incoming JSON text.
		 */
		@Override
		public void flatMap(String value, Collector<String> out) throws Exception {
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
			// if ((isEnglish) && hasText) {
			if ((isEnglish || isPortuguese || isSpanish || isGerman) && hasText) {
				// System.out.println(jsonNode.get("text").asText());
				// message of tweet
				StringTokenizer tokenizer = new StringTokenizer(jsonNode.get("text").asText());

				// split the message
				while (tokenizer.hasMoreTokens()) {
					String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();
					if (!result.isEmpty()) {
						out.collect(result);
					}
				}
			}
		}
	}

	public static class TimeOutDistinctAndCountFunction extends KeyedProcessFunction<Byte, String, String> {
		private static final long serialVersionUID = 4158912654767215147L;
		// delay after which an alert flag is thrown
		private final long timeOut;
		// state to remember the last timer set
		private transient ValueState<HyperLogLogState> hllStateTwitter;

		public TimeOutDistinctAndCountFunction(long timeOut) {
			this.timeOut = timeOut;
		}

		@Override
		public void open(Configuration conf) {
			// setup timer and HLL state
			ValueStateDescriptor<HyperLogLogState> descriptorHLL = new ValueStateDescriptor<>("hllStateTwitter",
					HyperLogLogState.class);
			hllStateTwitter = getRuntimeContext().getState(descriptorHLL);
		}

		@Override
		public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
			// get current time and compute timeout time
			long currentTime = ctx.timerService().currentProcessingTime();
			long timeoutTime = currentTime + timeOut;

			// update HLL state
			HyperLogLogState currentHLLState = hllStateTwitter.value();
			if (currentHLLState == null) {
				System.out.println("process: " + currentTime + " - " + timeoutTime);
				currentHLLState = new HyperLogLogState();
				// register timer for timeout time
				ctx.timerService().registerProcessingTimeTimer(timeoutTime);
			}
			currentHLLState.offer(value);

			// remember timeout time
			currentHLLState.setLastTimer(timeoutTime);
			hllStateTwitter.update(currentHLLState);
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
			// check if this was the last timer we registered
			HyperLogLogState currentHLLState = hllStateTwitter.value();
			System.out.println("onTimer: " + timestamp + " - " + currentHLLState.getLastTimer() + " = "
					+ (currentHLLState.getLastTimer() - timestamp));
			// it was, so no data was received afterwards. fire an alert.
			out.collect("estimate cardinality: " + currentHLLState.cardinality() + " real cardinality: "
					+ currentHLLState.getValues().size());
			hllStateTwitter.clear();
		}
	}
}

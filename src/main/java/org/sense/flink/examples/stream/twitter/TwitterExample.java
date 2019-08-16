package org.sense.flink.examples.stream.twitter;

import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

public class TwitterExample {

	public static void main(String[] args) throws Exception {
		// String[] parameters = parameter.split(" ");
		new TwitterExample(args);
	}

	public TwitterExample(String[] args) throws Exception {
		if (args == null) {
			return;
		}
		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		env.getConfig().disableSysoutLogging();

		// @formatter:off
		Properties props = new Properties();
		props.setProperty(TwitterSource.CONSUMER_KEY, "******************");
		props.setProperty(TwitterSource.CONSUMER_SECRET, "******************");
		props.setProperty(TwitterSource.TOKEN, "******************");
		props.setProperty(TwitterSource.TOKEN_SECRET, "******************");
		DataStream<String> streamSource = env.addSource(new TwitterSource(props));

		DataStream<Tuple2<String, Integer>> tweets = streamSource
				// selecting English tweets and splitting to (word, 1)
				.flatMap(new SelectEnglishAndTokenizeFlatMap())
				// group by words and sum their occurrences
				.keyBy(0)
				// window
				.window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
				// aggregate
				.sum(1);
		// @formatter:on

		tweets.print();

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("TwitterExample");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Deserialize JSON from twitter source
	 *
	 * <p>
	 * Implements a string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and splits
	 * it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static class SelectEnglishAndTokenizeFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		private transient ObjectMapper jsonParser;

		/**
		 * Select the language from the incoming JSON text.
		 */
		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}
			JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
			// boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang")
			// && jsonNode.get("user").get("lang").asText().equals("en");
			boolean isEnglish = jsonNode.has("lang") && jsonNode.get("lang").asText().equalsIgnoreCase("en");
			boolean hasText = jsonNode.has("text");
			if (isEnglish && hasText) {
				// if (hasText) {
				// message of tweet
				StringTokenizer tokenizer = new StringTokenizer(jsonNode.get("text").asText());

				// split the message
				while (tokenizer.hasMoreTokens()) {
					String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();

					if (!result.equals("")) {
						out.collect(new Tuple2<>(result, 1));
					}
				}
			}
		}
	}
}

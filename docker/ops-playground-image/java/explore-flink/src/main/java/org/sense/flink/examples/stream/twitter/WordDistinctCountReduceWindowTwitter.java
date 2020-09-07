package org.sense.flink.examples.stream.twitter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

/**
 * On the terminal execute "nc -lk 9000", run this class and type words back on
 * the terminal.
 * 
 * This program does the count distinct like in SQL. It first distinct all
 * worlds and then counts the number of words. In other words it does not count
 * the duplicates words in a data stream.
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class WordDistinctCountReduceWindowTwitter {

	public static void main(String[] args) throws Exception {
		new WordDistinctCountReduceWindowTwitter();
	}

	public WordDistinctCountReduceWindowTwitter() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		Time time = Time.seconds(5);

		// @formatter:off
		Properties props = new Properties();
		props.setProperty(TwitterSource.CONSUMER_KEY, "******************");
		props.setProperty(TwitterSource.CONSUMER_SECRET, "******************");
		props.setProperty(TwitterSource.TOKEN, "******************");
		props.setProperty(TwitterSource.TOKEN_SECRET, "******************");
		DataStream<String> streamSource = env.addSource(new TwitterSource(props));

		// DataStream<Tuple3<Integer, Long, String>> tweets = 
		streamSource
				.flatMap(new SelectWordsAndTokenizeFlatMap())
				.keyBy(new WordKeySelector())
				.timeWindow(time)
				.reduce(new DistinctReduceFunction())
				.timeWindowAll(time)
				.reduce(new CountReduceFunction())
				.map(new SwapWordAddTimeMapFunction())
				.print();
				;
		// @formatter:on

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		env.execute("WordDistinctCountReduceWindowTwitter");
	}

	public static class SelectWordsAndTokenizeFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = -7130548585178892323L;
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
						out.collect(new Tuple2<String, Integer>(result, 1));
					}
				}
			}
		}
	}

	public static class WordKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
		private static final long serialVersionUID = 2787589690596587044L;

		@Override
		public String getKey(Tuple2<String, Integer> value) throws Exception {
			return value.f0;
		}
	}

	public static class DistinctReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = -5389931813617389139L;

		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2)
				throws Exception {
			return Tuple2.of(value1.f0, value1.f1);
		}
	}

	public static class CountReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 8047191633772408164L;

		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2)
				throws Exception {
			return Tuple2.of(value1.f0 + "-" + value2.f0, value1.f1 + value2.f1);
		}
	}

	public static class SwapWordAddTimeMapFunction
			implements MapFunction<Tuple2<String, Integer>, Tuple3<String, String, String>> {
		private static final long serialVersionUID = 6708542346200344416L;
		private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

		@Override
		public Tuple3<String, String, String> map(Tuple2<String, Integer> value) throws Exception {
			return Tuple3.of("Distinct words: " + value.f1, sdf.format(new Date()), value.f0);
		}
	}
}

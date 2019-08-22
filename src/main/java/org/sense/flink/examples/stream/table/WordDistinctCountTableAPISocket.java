package org.sense.flink.examples.stream.table;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.OverWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
// import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

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
public class WordDistinctCountTableAPISocket {

	public static void main(String[] args) throws Exception {
		new WordDistinctCountTableAPISocket();
	}

	public WordDistinctCountTableAPISocket() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// @formatter:off
		DataStreamSource<String> ds = env.socketTextStream("localhost", 9000);
		tableEnv.registerDataStream("sourceTable", ds, "line, proctime.proctime");
		
		tableEnv.registerFunction("splitFunc", new SplitTableFunction());
		Table result = tableEnv.scan("sourceTable")
				.joinLateral("splitFunc(line) as word")
				// .window(Tumble.over("5.seconds").on("proctime").as("w"))
				// .window([OverWindow w].as("w"))
				// .groupBy("w")
				.select("count.distinct(word)");
		tableEnv.toAppendStream(result, Row.class).print();
		// @formatter:on

		System.out.println("Execution plan ........................ ");
		System.out.println(env.getExecutionPlan());
		System.out.println("Plan explaination ........................ ");
		System.out.println(tableEnv.explain(result));
		System.out.println("........................ ");

		env.execute("WordDistinctCountTableAPISocket");
	}

	public static class SplitTableFunction extends TableFunction<String> {
		private static final long serialVersionUID = 1218825097353746337L;

		public void eval(String line) {
			String[] words = line.split(" ");
			for (int i = 0; i < words.length; ++i) {
				collect(words[i]);
			}
		}
	}
}

package org.sense.flink.examples.stream.valencia;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.examples.stream.udf.impl.ValenciaItemDistrictMap;
import org.sense.flink.source.ValenciaItemConsumer;
import org.sense.flink.util.ValenciaItemType;

/**
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaTrafficJamSocket {

	public static void main(String[] args) throws Exception {
		new ValenciaTrafficJamSocket();
	}

	public ValenciaTrafficJamSocket() throws Exception {
		long frequencyMilliSeconds = Time.seconds(5).toMilliseconds();
		long duration = Long.MAX_VALUE;
		boolean collectWithTimestamp = true;
		boolean skewedDataInjection = true;
		boolean pinningPolicy = true;
		boolean offlineData = true;

		// @formatter:off
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		env.addSource(new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, frequencyMilliSeconds, collectWithTimestamp, offlineData, skewedDataInjection, duration, pinningPolicy)).name(ValenciaItemConsumer.class.getSimpleName())
			.map(new ValenciaItemDistrictMap(pinningPolicy)).name(ValenciaItemDistrictMap.class.getSimpleName())
			.print().name("print");

		disclaimer(env.getExecutionPlan());
		JobExecutionResult jobExecution = env.execute(ValenciaTrafficJamSocket.class.getSimpleName());
		System.out.println("Time [seconds] to execute     : " + jobExecution.getNetRuntime(TimeUnit.SECONDS) + " seconds");
		System.out.println("Time [milliseconds] to execute: " + jobExecution.getNetRuntime(TimeUnit.MILLISECONDS) + " milliseconds");
		// @formatter:on
	}

	private void disclaimer(String logicalPlan) {
		// @formatter:off
		System.out.println("This is the application [" + ValenciaTrafficJamSocket.class.getSimpleName() + "].");
		System.out.println("Use the 'Flink Plan Visualizer' [https://flink.apache.org/visualizer/] in order to see the logical plan of this application.");
		System.out.println("Logical plan >>>");
		System.err.println(logicalPlan);
		System.out.println();
		// @formatter:on
	}
}

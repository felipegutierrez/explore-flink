package org.sense.flink.examples.stream;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.sense.flink.mqtt.FlinkMqttByteConsumer;
import org.sense.flink.mqtt.MqttByteMessage;
import org.sense.flink.source.CameraMqttSource;
import org.sense.flink.source.TemperatureMqttSource;
import org.sense.flink.util.CameraSensor;
import org.sense.flink.util.TemperatureSensor;

public class SensorsReadingMqttJoinQEP {

	public SensorsReadingMqttJoinQEP() throws Exception {

		int timesRequestedTemperature = 50;
		int timesRequestedCamera = 50;

		// start the fake sensors
		CompletableFuture<String> temperatureFuture = startTemperatureSensor(timesRequestedTemperature);
		CompletableFuture<String> cameraFuture = startCameraSensor(timesRequestedCamera);

		// Start streaming from fake data source sensors
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<TemperatureSensor> dsTemp = env.addSource(new FlinkMqttByteConsumer("topic-temp"))
				.map(new TemperatureMqttMapper());
		DataStream<CameraSensor> dsCame = env.addSource(new FlinkMqttByteConsumer("topic-camera"))
				.map(new CameraMqttMapper());

		String executionPlan = env.getExecutionPlan();
		System.out.println("ExecutionPlan ........................ ");
		System.out.println(executionPlan);
		System.out.println("........................ ");

		dsTemp.print();
		dsCame.print();

		env.execute("SensorsMqttJoinQEP");

		// temperatureFuture
		System.out.println(temperatureFuture.get());
		// cameraFuture
		System.out.println(cameraFuture.get());
	}

	public CompletableFuture<String> startTemperatureSensor(int times) throws InterruptedException, ExecutionException {
		return CompletableFuture.supplyAsync(() -> {
			// Simulate a long-running Job
			TemperatureMqttSource temperatureSensor = new TemperatureMqttSource(times);
			System.out.println("TemperatureSourceMqtt started");
			temperatureSensor.run();
			return "Result of the asynchronous TemperatureSourceMqtt computation";
		});
	}

	public CompletableFuture<String> startCameraSensor(int times) throws InterruptedException, ExecutionException {
		return CompletableFuture.supplyAsync(() -> {
			// Simulate a long-running Job
			CameraMqttSource cameraSensor = new CameraMqttSource(times);
			System.out.println("CameraSourceMqtt started");
			cameraSensor.run();
			return "Result of the asynchronous CameraSourceMqtt computation";
		});
	}

	public static class TemperatureMqttMapper implements MapFunction<MqttByteMessage, TemperatureSensor> {

		private static final long serialVersionUID = -8886900931647296471L;

		@Override
		public TemperatureSensor map(MqttByteMessage value) throws Exception {
			byte[] payload = value.getPayload();

			// Bytes to doubles
			int times = Double.SIZE / Byte.SIZE;
			double[] doubles = new double[payload.length / times];
			for (int i = 0; i < doubles.length; i++) {
				doubles[i] = ByteBuffer.wrap(payload, i * times, times).getDouble();
			}
			// System.out.println("Location[Lat " + doubles[0] + ", Lon " + doubles[1] +
			// ",Alt " + doubles[2] + "] temperature: " + doubles[3]);

			byte[] bytes = new byte[8];
			ByteBuffer.wrap(bytes).putDouble(doubles[3]);
			return new TemperatureSensor(doubles[0], doubles[1], doubles[2], ByteBuffer.wrap(bytes));
		}
	}

	public static class CameraMqttMapper implements MapFunction<MqttByteMessage, CameraSensor> {

		private static final long serialVersionUID = 9073773958445611407L;

		@Override
		public CameraSensor map(MqttByteMessage value) throws Exception {
			byte[] payload = value.getPayload();

			// Bytes to doubles
			int times = Double.SIZE / Byte.SIZE;
			double[] doubles = new double[payload.length / times];
			for (int i = 0; i < doubles.length; i++) {
				doubles[i] = ByteBuffer.wrap(payload, i * times, times).getDouble();
			}
			// System.out.println("Location[Lat " + doubles[0] + ", Lon " + doubles[1] + ",
			// Alt " + doubles[2] + "] Camera: " + doubles[3]);

			byte[] bytes = new byte[8];
			ByteBuffer.wrap(bytes).putDouble(doubles[3]);
			return new CameraSensor(doubles[0], doubles[1], doubles[2], ByteBuffer.wrap(bytes));
		}
	}
}

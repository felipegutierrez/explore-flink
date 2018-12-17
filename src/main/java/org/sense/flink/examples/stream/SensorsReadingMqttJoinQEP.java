package org.sense.flink.examples.stream;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.sense.flink.source.CameraSourceMqtt;
import org.sense.flink.source.TemperatureSourceMqtt;

public class SensorsReadingMqttJoinQEP {

	public SensorsReadingMqttJoinQEP() throws Exception {

		int timesRequestedTemperature = 50;
		int timesRequestedCamera = 50;

		CompletableFuture<String> temperatureFuture = startTemperatureSensor(timesRequestedTemperature);
		CompletableFuture<String> cameraFuture = startCameraSensor(timesRequestedCamera);

		// temperatureFuture
		System.out.println(temperatureFuture.get());
		System.out.println(cameraFuture.get());
	}

	public CompletableFuture<String> startTemperatureSensor(int times) throws InterruptedException, ExecutionException {
		return CompletableFuture.supplyAsync(() -> {
			// Simulate a long-running Job
			TemperatureSourceMqtt temperatureSensor = new TemperatureSourceMqtt(times);
			System.out.println("TemperatureSourceMqtt started");
			temperatureSensor.run();
			return "Result of the asynchronous computation";
		});
	}

	public CompletableFuture<String> startCameraSensor(int times) throws InterruptedException, ExecutionException {
		return CompletableFuture.supplyAsync(() -> {
			// Simulate a long-running Job
			CameraSourceMqtt cameraSensor = new CameraSourceMqtt(times);
			System.out.println("CameraSourceMqtt started");
			cameraSensor.run();
			return "Result of the asynchronous computation";
		});
	}
}

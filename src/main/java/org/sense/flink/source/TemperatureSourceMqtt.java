package org.sense.flink.source;

import java.util.concurrent.TimeUnit;

import org.sense.flink.util.ISensor;
import org.sense.flink.util.TemperatureSensor;

/**
 * The temperature sensor returns the temperature of a specific location in the
 * format Camera<locantion<latitude, longitude, altitude>, temperatureValue>
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class TemperatureSourceMqtt {

	private int times;
	private ISensor sensor;

	public TemperatureSourceMqtt(int times) {
		this.times = times;
		this.sensor = new TemperatureSensor();
	}

	public void run() {
		try {
			for (int i = 0; i < times; i++) {
				this.sensor.readRequest();
				this.sensor.printReadRequest();
				TimeUnit.SECONDS.sleep(2);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

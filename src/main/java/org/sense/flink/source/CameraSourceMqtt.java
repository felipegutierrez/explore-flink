package org.sense.flink.source;

import java.util.concurrent.TimeUnit;

import org.sense.flink.util.CameraSensor;
import org.sense.flink.util.ISensor;

/**
 * The camera is not a static. It return the position where it is recording in
 * the following format Camera<locantion<latitude, longitude, altitude>,
 * VideoRecorded>
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class CameraSourceMqtt {

	private int times;
	private ISensor sensor;

	public CameraSourceMqtt(int times) {
		this.times = times;
		this.sensor = new CameraSensor();
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

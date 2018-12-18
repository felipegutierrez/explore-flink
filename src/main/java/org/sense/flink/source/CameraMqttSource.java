package org.sense.flink.source;

import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import org.fusesource.mqtt.client.Future;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.sense.flink.sensor.CameraSensor;
import org.sense.flink.sensor.ISensor;

/**
 * The camera is not a static. It return the position where it is recording in
 * the following format Camera<locantion<latitude, longitude, altitude>,
 * VideoRecorded>
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class CameraMqttSource {

	private int times;
	private ISensor sensor;
	private MQTT mqtt;

	public CameraMqttSource(int times) {
		this.times = times;
		this.sensor = new CameraSensor();
		this.mqtt = new MQTT();
	}

	public void run() {
		try {
			this.mqtt.setHost("127.0.0.1", 1883);
			FutureConnection connection = this.mqtt.futureConnection();
			Future<Void> f1 = connection.connect();
			f1.await();

			for (int i = 0; i < times; i++) {
				byte[] value = this.sensor.readRequest();
				Future<Void> f3 = connection.publish("topic-camera", value, QoS.AT_LEAST_ONCE, false);
				// this.sensor.printReadRequest();
				TimeUnit.SECONDS.sleep(1);
			}
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

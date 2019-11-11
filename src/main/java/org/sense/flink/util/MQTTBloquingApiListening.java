package org.sense.flink.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.concurrent.TimeUnit;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

public class MQTTBloquingApiListening extends Thread {

	private static final String TOPIC_FREQUENCY = "frequency-topic";
	private BlockingConnection subscriber;
	private MQTT mqtt;
	private String host;
	private int port;
	private int delay = 10000;
	private boolean running = false;

	public MQTTBloquingApiListening() {
		this("127.0.0.1", 1883);
	}

	public MQTTBloquingApiListening(String host, int port) {
		this.host = host;
		this.port = port;
		this.running = true;
		this.disclaimer();
	}

	public void connect() throws Exception {
		mqtt = new MQTT();
		mqtt.setHost(host, port);

		subscriber = mqtt.blockingConnection();
		subscriber.connect();
		Topic[] topics = new Topic[] { new Topic(TOPIC_FREQUENCY, QoS.AT_MOST_ONCE) };
		subscriber.subscribe(topics);
	}

	public void run() {
		try {
			while (running) {
				System.out.println("waiting for messages...");
				Message msg = subscriber.receive(1, TimeUnit.SECONDS);
				if (msg != null) {
					msg.ack();
					System.out.println(new String(msg.getPayload(), UTF_8));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void disconnect() throws Exception {
		subscriber.disconnect();
	}

	private boolean isInteger(String s) {
		return isInteger(s, 10);
	}

	private boolean isInteger(String s, int radix) {
		if (s.isEmpty())
			return false;
		for (int i = 0; i < s.length(); i++) {
			if (i == 0 && s.charAt(i) == '-') {
				if (s.length() == 1)
					return false;
				else
					continue;
			}
			if (Character.digit(s.charAt(i), radix) < 0)
				return false;
		}
		return true;
	}

	private void disclaimer() {
		// @formatter:off
		System.out.println("This is the application [" + MQTTBloquingApiListening.class.getSimpleName() + "].");
		System.out.println("It changes the frequency of a thread by publishing into an MQTT broker.");
		System.out.println("To change the frequency of emission use:");
		System.out.println("mosquitto_pub -h " + host + " -p " + port + " -t " + TOPIC_FREQUENCY + " -m \"miliseconds\"");
		System.out.println();
		// @formatter:on
	}

	public static void main(String[] args) throws Exception {
		MQTTBloquingApiListening restApiListening = new MQTTBloquingApiListening();
		restApiListening.connect();
		restApiListening.start();
		Thread.sleep(1000000);
		restApiListening.disconnect();
	}
}

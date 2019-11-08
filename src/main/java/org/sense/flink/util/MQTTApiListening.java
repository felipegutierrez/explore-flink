package org.sense.flink.util;

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.Callback;
import org.fusesource.mqtt.client.CallbackConnection;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

public class MQTTApiListening extends Thread {

	private static final String TOPIC_FREQUENCY = "frequency-topic";
	private CallbackConnection connectionSideParameter;
	private FutureConnection connection;
	private MQTT mqtt;
	private String host;
	private int port;
	private int delay = 10000;
	private boolean running = false;

	public MQTTApiListening() {
		this("127.0.0.1", 1883);
	}

	public MQTTApiListening(String host, int port) {
		this.host = host;
		this.port = port;
		this.disclaimer();
	}

	public void connect() throws Exception {
		mqtt = new MQTT();
		mqtt.setHost(host, port);

		connection = mqtt.futureConnection();
		connection.connect().await();
	}

	public void run() {
		connectionSideParameter = mqtt.callbackConnection();
		connectionSideParameter.listener(new org.fusesource.mqtt.client.Listener() {
			public void onConnected() {
			}

			public void onDisconnected() {
			}

			public void onFailure(Throwable value) {
				value.printStackTrace();
				System.exit(-2);
			}

			public void onPublish(UTF8Buffer topic, Buffer msg, Runnable ack) {
				String body = msg.utf8().toString();

				System.out.println("Reading new frequency parameter: " + body);
				if (isInteger(body)) {
					delay = Integer.parseInt(body);
				} else if ("SHUTDOWN".equalsIgnoreCase(body)) {
					running = false;
				} else {
					System.out.println(body);
				}
				ack.run();
			}
		});
		connectionSideParameter.connect(new Callback<Void>() {
			@Override
			public void onSuccess(Void value) {
				Topic[] topics = { new Topic(TOPIC_FREQUENCY, QoS.AT_LEAST_ONCE) };
				connectionSideParameter.subscribe(topics, new Callback<byte[]>() {
					public void onSuccess(byte[] qoses) {
					}

					public void onFailure(Throwable value) {
						value.printStackTrace();
						System.exit(-2);
					}
				});
			}

			@Override
			public void onFailure(Throwable value) {
				value.printStackTrace();
				System.exit(-2);
			}
		});

		// Wait forever..
		// synchronized (ValenciaDataProducer.class) {
		// while (true)
		// ValenciaDataProducer.class.wait();
		// }
	}

	public void disconnect() throws Exception {
		connection.disconnect().await();
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
		System.out.println("This is the application [" + MQTTApiListening.class.getSimpleName() + "].");
		System.out.println("It changes the frequency of a thread by publishing into an MQTT broker.");
		System.out.println("To change the frequency of emission use:");
		System.out.println("mosquitto_pub -h " + host + " -p " + port + " -t " + TOPIC_FREQUENCY + " -m \"miliseconds\"");
		System.out.println();
		// @formatter:on
	}

	public static void main(String[] args) throws Exception {
		MQTTApiListening restApiListening = new MQTTApiListening();
		restApiListening.connect();
		restApiListening.start();
		Thread.sleep(1000000);
		restApiListening.disconnect();
	}
}

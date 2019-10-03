package org.sense.flink.examples.stream.valencia;

import java.util.LinkedList;

import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.Callback;
import org.fusesource.mqtt.client.CallbackConnection;
import org.fusesource.mqtt.client.Future;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.sense.flink.util.ValenciaItemType;

public class ValenciaDataProducer extends Thread {

	private final static String TOPIC_PUBLISH_VALENCIA_TRAFFIC_JAM = "topic-valencia-traffic-jam";
	private final static String TOPIC_PUBLISH_VALENCIA_POLLUTION = "topic-valencia-pollution";
	private final static String TOPIC_SUBSCRIPTION_FREQUENCY_VALENCIA_TRAFFIC_JAM = "topic-valencia-traffic-jam-frequency";
	private final static String TOPIC_SUBSCRIPTION_FREQUENCY_VALENCIA_POLLUTION = "topic-valencia-traffic-pollution";

	private FutureConnection connection;
	private CallbackConnection connectionSideParameter;
	private MQTT mqtt;
	private int delay = 1000;
	private boolean running = false;
	private String host;
	private int port;
	private ValenciaItemType valenciaItemType;
	private String topicToPublish;
	private String topicFrequencyParameter;

	public ValenciaDataProducer(ValenciaItemType valenciaItemType) {
		this(valenciaItemType, "localhost", 1883);
	}

	public ValenciaDataProducer(ValenciaItemType valenciaItemType, String host) {
		this(valenciaItemType, host, 1883);
	}

	public ValenciaDataProducer(ValenciaItemType valenciaItemType, String host, int port) {
		this.valenciaItemType = valenciaItemType;
		this.host = host;
		this.port = port;
		this.running = true;
		if (valenciaItemType == ValenciaItemType.TRAFFIC_JAM) {
			this.topicToPublish = TOPIC_PUBLISH_VALENCIA_TRAFFIC_JAM;
			this.topicFrequencyParameter = TOPIC_SUBSCRIPTION_FREQUENCY_VALENCIA_TRAFFIC_JAM;
		} else if (valenciaItemType == ValenciaItemType.AIR_POLLUTION) {
			this.topicToPublish = TOPIC_PUBLISH_VALENCIA_POLLUTION;
			this.topicFrequencyParameter = TOPIC_SUBSCRIPTION_FREQUENCY_VALENCIA_POLLUTION;
		} else {
			System.out.println("Wrong topic to publish messages");
		}
		this.disclaimer();
	}

	public void connect() throws Exception {

		mqtt = new MQTT();
		mqtt.setHost(host, port);

		connection = mqtt.futureConnection();
		connection.connect().await();
	}

	public void run() {
		try {
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

					if (isInteger(body)) {
						System.out.println("Reading new frequency parameter: " + body);
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
					Topic[] topics = { new Topic(topicFrequencyParameter, QoS.AT_LEAST_ONCE) };
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
			synchronized (ValenciaDataProducer.class) {
				while (true)
					ValenciaDataProducer.class.wait();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void publish() throws Exception {
		int messages = 10000;
		int size = 256;
		String DATA = "abcdefghijklmnopqrstuvwxyz";
		String body = "";
		for (int i = 0; i < size; i++) {
			body += DATA.charAt(i % DATA.length());
		}
		Buffer msg = new AsciiBuffer(body);

		final LinkedList<Future<Void>> queue = new LinkedList<Future<Void>>();
		UTF8Buffer topic = new UTF8Buffer(topicToPublish);

		while (running) {
			// Send the publish without waiting for it to complete. This allows us
			// to send multiple message without blocking..
			queue.add(connection.publish(topic, msg, QoS.AT_LEAST_ONCE, false));

			// Eventually we start waiting for old publish futures to complete
			// so that we don't create a large in memory buffer of outgoing message.s
			if (queue.size() >= 1000) {
				queue.removeFirst().await();
			}
			Thread.sleep(delay);
		}

		queue.add(connection.publish(topic, new AsciiBuffer("SHUTDOWN"), QoS.AT_LEAST_ONCE, false));
		while (!queue.isEmpty()) {
			queue.removeFirst().await();
		}
	}

	public void disconnect() throws Exception {
		connection.disconnect().await();
	}

	public boolean isInteger(String s) {
		return isInteger(s, 10);
	}

	public boolean isInteger(String s, int radix) {
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
		System.out.println("This is the application [" + ValenciaDataProducer.class.getSimpleName() + "].");
		System.out.println("It aims to collect online or offiline data from Valencia Open-data web portal and forward it to a mqtt channel with the possibility to vary the frequency of emmision.");
		System.out.println("To consume data on the terminal use:");
		System.out.println("mosquitto_sub -h " + host + " -p " + port + " -t " + topicToPublish);
		System.out.println("To change the frequency of emission use:");
		System.out.println("mosquitto_pub -h " + host + " -p " + port + " -t " + topicFrequencyParameter + " -m \"miliseconds\"");
		System.out.println();
		// @formatter:on
	}

	public static void main(String[] args) throws Exception {

		ValenciaDataProducer producer = new ValenciaDataProducer(ValenciaItemType.TRAFFIC_JAM);
		producer.connect();
		producer.start();
		producer.publish();
		producer.disconnect();
	}
}

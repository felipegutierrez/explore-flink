package org.sense.flink.mqtt;

import java.util.LinkedList;

import org.apache.flink.configuration.Configuration;
// import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
// import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.Future;
import org.fusesource.mqtt.client.FutureConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.sense.flink.examples.stream.udfs.PrinterSink;

public class MqttStringPublisher extends RichSinkFunction<String> {
	private static final long serialVersionUID = 1736047291991894958L;

	String user = env("ACTIVEMQ_USER", "admin");
	String password = env("ACTIVEMQ_PASSWORD", "password");
	String host = env("ACTIVEMQ_HOST", "localhost");
	int port = Integer.parseInt(env("ACTIVEMQ_PORT", "1883"));
	// final String destination = arg(null, 0, "/topic/event");
	FutureConnection connection;

	private static final String DEFAUL_HOST = "127.0.0.1";
	private static final int DEFAUL_PORT = 1883;

	private String topic;
	private QoS qos;

	// Create metrics
	// private transient Counter counter;
	// private transient Meter meter;

	public MqttStringPublisher(String topic) {
		this(DEFAUL_HOST, DEFAUL_PORT, topic, QoS.AT_LEAST_ONCE);
	}

	public MqttStringPublisher(String host, String topic) {
		this(host, DEFAUL_PORT, topic, QoS.AT_LEAST_ONCE);
	}

	public MqttStringPublisher(String host, int port, String topic) {
		this(host, port, topic, QoS.AT_LEAST_ONCE);
	}

	public MqttStringPublisher(String host, int port, String topic, QoS qos) {
		this.host = host;
		this.port = port;
		this.topic = topic;
		this.qos = qos;
	}

	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);
		// this.counter=getRuntimeContext().getMetricGroup().counter("counterSensorTypeMapper");
		// com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
		// this.meter = getRuntimeContext().getMetricGroup().meter(MqttStringPublisher.class.getSimpleName() + "-meter",
		// new DropwizardMeterWrapper(dropwizardMeter));

		// Open the MQTT connection just once
		MQTT mqtt = new MQTT();
		mqtt.setHost(host, port);
		mqtt.setUserName(user);
		mqtt.setPassword(password);

		connection = mqtt.futureConnection();
		connection.connect().await();
	}

	@Override
	public void close() throws Exception {
		super.close();
		connection.disconnect().await();
	}

	@Override
	public void invoke(String value) throws Exception {
		// this.meter.markEvent();
		// this.counter.inc();
		System.out.println(PrinterSink.class.getSimpleName() + ": " + value);
		System.out.flush();

		int size = value.toString().length();
		String DATA = value.toString();
		String body = "";
		for (int i = 0; i < size; i++) {
			body += DATA.charAt(i % DATA.length());
		}
		Buffer msg = new AsciiBuffer(body);

		final LinkedList<Future<Void>> queue = new LinkedList<Future<Void>>();
		UTF8Buffer topic = new UTF8Buffer(this.topic);
		// UTF8Buffer topic = new UTF8Buffer(destination);

		// Send the publish without waiting for it to complete. This allows us
		// to send multiple message without blocking..
		queue.add(connection.publish(topic, msg, this.qos, false));
		// queue.add(connection.publish(topic, msg, QoS.AT_LEAST_ONCE, false));
		// Eventually we start waiting for old publish futures to complete
		// so that we don't create a large in memory buffer of outgoing message.s
		if (queue.size() >= 1000) {
			queue.removeFirst().await();
		}

		while (!queue.isEmpty()) {
			queue.removeFirst().await();
		}
	}

	private static String env(String key, String defaultValue) {
		String rc = System.getenv(key);
		if (rc == null)
			return defaultValue;
		return rc;
	}

	private static String arg(String[] args, int index, String defaultValue) {
		if (args != null && index < args.length)
			return args[index];
		else
			return defaultValue;
	}
}

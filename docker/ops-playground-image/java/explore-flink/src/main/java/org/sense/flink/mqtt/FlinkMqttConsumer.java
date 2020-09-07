package org.sense.flink.mqtt;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * This is a source for Mqtt.
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class FlinkMqttConsumer extends RichSourceFunction<MqttMessage> {

	private static final long serialVersionUID = 5263423225238341915L;

	final private static String DEFAUL_HOST = "127.0.0.1";
	final private static int DEFAUL_PORT = 1883;

	private String host;
	private int port;
	private String topic;
	private QoS qos;

	public FlinkMqttConsumer(String topic) {
		this(DEFAUL_HOST, DEFAUL_PORT, topic, QoS.AT_LEAST_ONCE);
	}

	public FlinkMqttConsumer(String host, String topic) {
		this(host, DEFAUL_PORT, topic, QoS.AT_LEAST_ONCE);
	}

	public FlinkMqttConsumer(String host, int port, String topic) {
		this(host, port, topic, QoS.AT_LEAST_ONCE);
	}

	public FlinkMqttConsumer(String host, int port, String topic, QoS qos) {
		this.host = host;
		this.port = port;
		this.topic = topic;
		this.qos = qos;
		this.disclaimer();
	}

	private void disclaimer() {
		// System.out.println("In order to publish a parameter to this application use
		// the command line below:");
		// System.out.println("mosquitto_pub -h " + host + " -t " + topic + "
		// <MESSAGE>");
	}

	@Override
	public void run(SourceContext<MqttMessage> ctx) throws Exception {
		MQTT mqtt = new MQTT();
		mqtt.setHost(host, port);
		BlockingConnection blockingConnection = mqtt.blockingConnection();
		blockingConnection.connect();

		byte[] qoses = blockingConnection.subscribe(new Topic[] { new Topic(topic, qos) });

		while (blockingConnection.isConnected()) {
			Message message = blockingConnection.receive();
			MqttMessage mqttMessage = new MqttMessage(message.getTopic(), new String(message.getPayload()));
			message.ack();
			ctx.collect(mqttMessage);
		}
		blockingConnection.disconnect();
	}

	@Override
	public void cancel() {
		// TODO Auto-generated method stub
	}
}

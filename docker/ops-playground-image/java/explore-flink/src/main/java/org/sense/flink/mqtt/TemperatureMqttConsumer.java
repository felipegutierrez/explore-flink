package org.sense.flink.mqtt;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * This is a source for Mqtt temperature with ID of the sensor.
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class TemperatureMqttConsumer extends RichSourceFunction<MqttTemperature> {

	private static final long serialVersionUID = -5573754575893338495L;
	final private static String DEFAUL_HOST = "127.0.0.1";
	final private static int DEFAUL_PORT = 1883;

	private String host;
	private int port;
	private String topic;
	private QoS qos;

	public TemperatureMqttConsumer(String topic) {
		this(DEFAUL_HOST, DEFAUL_PORT, topic, QoS.AT_LEAST_ONCE);
	}

	public TemperatureMqttConsumer(String host, String topic) {
		this(host, DEFAUL_PORT, topic, QoS.AT_LEAST_ONCE);
	}

	public TemperatureMqttConsumer(String host, int port, String topic) {
		this(host, port, topic, QoS.AT_LEAST_ONCE);
	}

	public TemperatureMqttConsumer(String host, int port, String topic, QoS qos) {
		this.host = host;
		this.port = port;
		this.topic = topic;
		this.qos = qos;
	}

	@Override
	public void run(SourceContext<MqttTemperature> ctx) throws Exception {
		MQTT mqtt = new MQTT();
		mqtt.setHost(host, port);
		BlockingConnection blockingConnection = mqtt.blockingConnection();
		blockingConnection.connect();

		byte[] qoses = blockingConnection.subscribe(new Topic[] { new Topic(topic, qos) });

		while (blockingConnection.isConnected()) {
			Message message = blockingConnection.receive();
			String payload = new String(message.getPayload());
			String[] arr = payload.split("\\|");
			MqttTemperature mqttMessage = new MqttTemperature(message.getTopic(), Integer.valueOf(arr[0]),
					Double.valueOf(arr[1]));
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

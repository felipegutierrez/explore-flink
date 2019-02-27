package org.sense.flink.mqtt;

import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;

/**
 * This is a source for Mqtt sensor with ID of the sensor.
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class MqttSensorConsumer extends RichSourceFunction<MqttSensor> {

	private static final long serialVersionUID = -1384636057411239133L;
	final private static String DEFAUL_HOST = "127.0.0.1";
	final private static int DEFAUL_PORT = 1883;

	private String host;
	private int port;
	private String topic;
	private QoS qos;

	public MqttSensorConsumer(String topic) {
		this(DEFAUL_HOST, DEFAUL_PORT, topic, QoS.AT_LEAST_ONCE);
	}

	public MqttSensorConsumer(String host, String topic) {
		this(host, DEFAUL_PORT, topic, QoS.AT_LEAST_ONCE);
	}

	public MqttSensorConsumer(String host, int port, String topic) {
		this(host, port, topic, QoS.AT_LEAST_ONCE);
	}

	public MqttSensorConsumer(String host, int port, String topic, QoS qos) {
		this.host = host;
		this.port = port;
		this.topic = topic;
		this.qos = qos;
	}

	@Override
	public void run(SourceContext<MqttSensor> ctx) throws Exception {
		MQTT mqtt = new MQTT();
		mqtt.setHost(host, port);
		BlockingConnection blockingConnection = mqtt.blockingConnection();
		blockingConnection.connect();

		byte[] qoses = blockingConnection.subscribe(new Topic[] { new Topic(topic, qos) });

		while (blockingConnection.isConnected()) {
			Message message = blockingConnection.receive();
			String payload = new String(message.getPayload());
			String[] arr = payload.split("\\|");

			// @formatter:off
			// 11|COUNT_PE|2|CIT|1|18
			// @formatter:on

			Integer sensorId = 0;
			String sensorType = "";
			Integer platformId = 0;
			String platformType = "";
			Integer stationId = 0;
			Double value = 0.0;
			try {
				sensorId = Integer.parseInt(arr[0]);
			} catch (NumberFormatException re) {
				// System.err.println("Error converting arr0.");
			}
			try {
				sensorType = String.valueOf(arr[1]);
			} catch (ClassCastException re) {
				// System.err.println("Error converting arr1.");
			}
			try {
				platformId = Integer.parseInt(arr[2]);
			} catch (NumberFormatException re) {
				// System.err.println("Error converting arr2.");
			}
			try {
				platformType = String.valueOf(arr[3]);
			} catch (ClassCastException re) {
				// System.err.println("Error converting arr3.");
			}
			try {
				stationId = Integer.parseInt(arr[4]);
			} catch (NumberFormatException re) {
				// System.err.println("Error converting arr3.");
			}
			try {
				value = Double.parseDouble(arr[5]);
			} catch (NumberFormatException re) {
				// System.err.println("Error converting arr5.");
			}

			Tuple5<Integer, String, Integer, String, Integer> key = new Tuple5<Integer, String, Integer, String, Integer>(
					sensorId, sensorType, platformId, platformType, stationId);

			MqttSensor mqttMessage = new MqttSensor(message.getTopic(), key, value);
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

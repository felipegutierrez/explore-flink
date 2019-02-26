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
			// SensorKey [id=17, sensorType=TEMPERATURE, platform=Platform [id=1, station=Station [id=2, platforms=null]]]|25.13643935135741
			// @formatter:on

			Integer arr0 = 0;
			String arr1 = "";
			Integer arr2 = 0;
			String arr3 = "";
			Integer arr4 = 0;
			Double arr5 = 0.0;
			try {
				arr0 = Integer.parseInt(arr[0]);
			} catch (NumberFormatException re) {
				// System.err.println("Error converting arr0.");
			}
			try {
				arr1 = String.valueOf(arr[1]);
			} catch (ClassCastException re) {
				// System.err.println("Error converting arr1.");
			}
			try {
				arr2 = Integer.parseInt(arr[2]);
			} catch (NumberFormatException re) {
				// System.err.println("Error converting arr2.");
			}
			try {
				arr3 = String.valueOf(arr[3]);
			} catch (ClassCastException re) {
				// System.err.println("Error converting arr3.");
			}
			try {
				arr4 = Integer.parseInt(arr[4]);
			} catch (NumberFormatException re) {
				// System.err.println("Error converting arr3.");
			}
			try {
				arr5 = Double.parseDouble(arr[5]);
			} catch (NumberFormatException re) {
				// System.err.println("Error converting arr5.");
			}

			Tuple5<Integer, String, Integer, String, Integer> key = new Tuple5<Integer, String, Integer, String, Integer>(
					arr0, arr1, arr2, arr3, arr4);

			MqttSensor mqttMessage = new MqttSensor(message.getTopic(), key, arr5);
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

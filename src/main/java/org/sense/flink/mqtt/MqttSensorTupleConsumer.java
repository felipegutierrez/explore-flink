package org.sense.flink.mqtt;

import org.apache.flink.api.java.tuple.Tuple8;
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
public class MqttSensorTupleConsumer
		extends RichSourceFunction<Tuple8<Integer, String, Integer, String, Integer, Long, Double, String>> {
	private static final long serialVersionUID = -2306664866154656176L;
	private static final String DEFAUL_HOST = "127.0.0.1";
	private static final int DEFAUL_PORT = 1883;

	private String host;
	private int port;
	private String topic;
	private QoS qos;

	public MqttSensorTupleConsumer(String topic) {
		this(DEFAUL_HOST, DEFAUL_PORT, topic, QoS.AT_LEAST_ONCE);
	}

	public MqttSensorTupleConsumer(String host, String topic) {
		this(host, DEFAUL_PORT, topic, QoS.AT_LEAST_ONCE);
	}

	public MqttSensorTupleConsumer(String host, int port, String topic) {
		this(host, port, topic, QoS.AT_LEAST_ONCE);
	}

	public MqttSensorTupleConsumer(String host, int port, String topic, QoS qos) {
		this.host = host;
		this.port = port;
		this.topic = topic;
		this.qos = qos;
	}

	@Override
	public void run(SourceContext<Tuple8<Integer, String, Integer, String, Integer, Long, Double, String>> ctx)
			throws Exception {
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
			// 11      | COUNT_PE  | 2         | CIT         | 1        | timestamp| 18   | Berlin-Paris
			// sensorId, sensorType, platformId, platformType, stationId, timestamp, value, trip
			// @formatter:on

			Integer sensorId = 0;
			String sensorType = "";
			Integer platformId = 0;
			String platformType = "";
			Integer stationId = 0;
			Long timestamp = 0L;
			Double value = 0.0;
			String trip = "";
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
				timestamp = Long.parseLong(arr[5]);
			} catch (NumberFormatException re) {
				// System.err.println("Error converting arr5.");
			}
			try {
				value = Double.parseDouble(arr[6]);
			} catch (NumberFormatException re) {
				// System.err.println("Error converting arr6.");
			}
			try {
				trip = String.valueOf(arr[7]);
			} catch (ClassCastException re) {
				// System.err.println("Error converting arr6.");
			}

			// KEY = sensorId, sensorType, platformId, platformType, stationId
			// VALUE = timestamp, value, trip
			Tuple8<Integer, String, Integer, String, Integer, Long, Double, String> tuple8 = Tuple8.of(sensorId,
					sensorType, platformId, platformType, stationId, timestamp, value, trip);

			message.ack();
			ctx.collect(tuple8);
		}
		blockingConnection.disconnect();
	}

	@Override
	public void cancel() {
	}
}

package org.sense.flink.mqtt;

import static org.sense.flink.util.ValenciaMqttTopics.TOPIC_VALENCIA_NOISE;
import static org.sense.flink.util.ValenciaMqttTopics.TOPIC_VALENCIA_POLLUTION;
import static org.sense.flink.util.ValenciaMqttTopics.TOPIC_VALENCIA_TRAFFIC_JAM;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.pojo.ValenciaPollution;
import org.sense.flink.pojo.ValenciaTraffic;
import org.sense.flink.util.CpuGauge;
import org.sense.flink.util.ValenciaItemType;

import net.openhft.affinity.impl.LinuxJNAAffinity;

/**
 * This is a source for Mqtt sensor with ID of the sensor.
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class MqttValenciaItemConsumer extends RichSourceFunction<ValenciaItem> {

	private static final long serialVersionUID = -1384636057411239133L;
	private static final String DEFAUL_HOST = "127.0.0.1";
	private static final int DEFAUL_PORT = 1883;

	private String host;
	private int port;
	private String topic;
	private QoS qos;
	private ValenciaItemType valenciaItemType;
	private boolean collectWithTimestamp;
	// private transient int cpuCore;
	private transient CpuGauge cpuGauge;

	public MqttValenciaItemConsumer(ValenciaItemType valenciaItemType, boolean collectWithTimestamp) {
		this(DEFAUL_HOST, DEFAUL_PORT, QoS.AT_LEAST_ONCE, valenciaItemType, collectWithTimestamp);
	}

	public MqttValenciaItemConsumer(String host, ValenciaItemType valenciaItemType, boolean collectWithTimestamp) {
		this(host, DEFAUL_PORT, QoS.AT_LEAST_ONCE, valenciaItemType, collectWithTimestamp);
	}

	public MqttValenciaItemConsumer(String host, int port, ValenciaItemType valenciaItemType,
			boolean collectWithTimestamp) {
		this(host, port, QoS.AT_LEAST_ONCE, valenciaItemType, collectWithTimestamp);
	}

	public MqttValenciaItemConsumer(String host, int port, QoS qos, ValenciaItemType valenciaItemType,
			boolean collectWithTimestamp) {
		this.host = host;
		this.port = port;
		this.qos = qos;
		this.valenciaItemType = valenciaItemType;
		this.collectWithTimestamp = collectWithTimestamp;
		if (valenciaItemType == ValenciaItemType.TRAFFIC_JAM) {
			this.topic = TOPIC_VALENCIA_TRAFFIC_JAM;
		} else if (valenciaItemType == ValenciaItemType.AIR_POLLUTION) {
			this.topic = TOPIC_VALENCIA_POLLUTION;
		} else if (valenciaItemType == ValenciaItemType.NOISE) {
			this.topic = TOPIC_VALENCIA_NOISE;
		} else {
			System.out.println("Wrong topic to consume messages");
		}
		this.disclaimer();
	}

	@Override
	public void open(Configuration config) {
		this.cpuGauge = new CpuGauge();
		getRuntimeContext().getMetricGroup().gauge("cpu", cpuGauge);
//		getRuntimeContext().getMetricGroup().gauge("cpu", new Gauge<Integer>() {
//			@Override
//			public Integer getValue() {
//				return cpuCore;
//			}
//		});
	}

	private void disclaimer() {
		System.out.println("Use the following command to consume data from the application >>>");
		System.out.println("mosquitto_sub -h " + host + " -p " + port + " -t " + topic);
		System.out.println();
	}

	@Override
	public void run(SourceContext<ValenciaItem> ctx) throws Exception {
		MQTT mqtt = new MQTT();
		mqtt.setHost(host, port);
		BlockingConnection blockingConnection = mqtt.blockingConnection();
		blockingConnection.connect();

		byte[] qoses = blockingConnection.subscribe(new Topic[] { new Topic(topic, qos) });

		while (blockingConnection.isConnected()) {
			// updates the CPU core current in use
			// this.cpuCore = LinuxJNAAffinity.INSTANCE.getCpu();
			this.cpuGauge.updateValue(LinuxJNAAffinity.INSTANCE.getCpu());

			Message message = blockingConnection.receive();
			String payload = new String(message.getPayload());
			message.ack();

			// convert message to ValenciaItem
			Date eventTime = new Date();
			ObjectMapper mapper = new ObjectMapper();
			JsonNode actualObj = mapper.readTree(payload);

			boolean isCRS = actualObj.has("crs");
			boolean isFeatures = actualObj.has("features");
			String typeCSR = "";

			if (isCRS) {
				ObjectNode objectNodeCsr = (ObjectNode) actualObj.get("crs");
				ObjectNode objectNodeProperties = (ObjectNode) objectNodeCsr.get("properties");
				typeCSR = objectNodeProperties.get("name").asText();
				typeCSR = typeCSR.substring(typeCSR.indexOf("EPSG")).replace("::", ":");
			} else {
				System.out.println("Wrong CoordinateReferenceSystem (CSR) type");
			}

			if (isFeatures) {
				ArrayNode arrayNodeFeatures = (ArrayNode) actualObj.get("features");
				for (JsonNode jsonNode : arrayNodeFeatures) {
					JsonNode nodeProperties = jsonNode.get("properties");
					JsonNode nodeGeometry = jsonNode.get("geometry");
					ArrayNode arrayNodeCoordinates = (ArrayNode) nodeGeometry.get("coordinates");

					ValenciaItem valenciaItem;
					List<Point> points = new ArrayList<Point>();
					if (valenciaItemType == ValenciaItemType.TRAFFIC_JAM) {
						for (JsonNode coordinates : arrayNodeCoordinates) {
							ArrayNode xy = (ArrayNode) coordinates;
							points.add(new Point(xy.get(0).asDouble(), xy.get(1).asDouble(), typeCSR));
						}
						valenciaItem = new ValenciaTraffic(0L, 0L, "", eventTime, points,
								nodeProperties.get("estado").asInt());
					} else if (valenciaItemType == ValenciaItemType.AIR_POLLUTION) {
						String p = arrayNodeCoordinates.get(0).asText() + "," + arrayNodeCoordinates.get(1).asText();
						points = Point.extract(p, typeCSR);
						valenciaItem = new ValenciaPollution(0L, 0L, "", eventTime, points,
								nodeProperties.get("mediciones").asText());
					} else if (valenciaItemType == ValenciaItemType.NOISE) {
						throw new Exception("ValenciaItemType NOISE is not implemented!");
					} else {
						throw new Exception("ValenciaItemType is NULL!");
					}
					if (valenciaItem != null) {
						if (collectWithTimestamp) {
							ctx.collectWithTimestamp(valenciaItem, eventTime.getTime());
							// ctx.emitWatermark(new Watermark(eventTime.getTime()));
						} else {
							ctx.collect(valenciaItem);
						}
					}
				}
			}
		}
		blockingConnection.disconnect();
	}

	@Override
	public void cancel() {
	}
}

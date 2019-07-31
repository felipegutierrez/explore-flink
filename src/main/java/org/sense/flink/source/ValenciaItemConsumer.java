package org.sense.flink.source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.pojo.ValenciaPollution;
import org.sense.flink.pojo.ValenciaTraffic;
import org.sense.flink.util.ValenciaItemType;

/**
 * <pre>
 * The data comes from http://gobiernoabierto.valencia.es/en/dataset/?id=estado-trafico-tiempo-real and the attributes are:
 * 
 * Idtramo: Identificador único del tramo.
 * Denominacion: Denominación del tramo.
 * Estado: Estado del tráfico en tiempo real del tramo. Los códigos de los estados del tráfico son los siguientes:
 * 0 Fluido
 * 1 Denso
 * 2 Congestionado
 * 3 Cortado
 * </pre>
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaItemConsumer extends RichSourceFunction<ValenciaItem> {

	private static final long serialVersionUID = 8320419468972434516L;
	private static final String VALENCIA_TRAFFIC_JAM_URL = "http://mapas.valencia.es/lanzadera/opendata/Tra-estado-trafico/JSON";
	private static final String VALENCIA_POLLUTION_URL = "http://mapas.valencia.es/lanzadera/opendata/Estautomaticas/JSON";
	private static final long DEFAULT_FREQUENCY_DELAY = 10000;
	private String json;
	private long delayTime;
	private ValenciaItemType valenciaItemType;

	public ValenciaItemConsumer(ValenciaItemType valenciaItemType) throws Exception {
		if (valenciaItemType == ValenciaItemType.TRAFFIC) {
			this.json = VALENCIA_TRAFFIC_JAM_URL;
		} else if (valenciaItemType == ValenciaItemType.AIR_POLLUTION) {
			this.json = VALENCIA_POLLUTION_URL;
		} else if (valenciaItemType == ValenciaItemType.NOISE) {
			this.json = "";
		} else {
			throw new Exception("ValenciaItemType is NULL!");
		}
		this.valenciaItemType = valenciaItemType;
		this.delayTime = DEFAULT_FREQUENCY_DELAY;
	}

	public ValenciaItemConsumer(ValenciaItemType valenciaItemType, String json) throws Exception {
		this(valenciaItemType, json, DEFAULT_FREQUENCY_DELAY);
	}

	public ValenciaItemConsumer(ValenciaItemType valenciaItemType, String json, long delayTime) throws Exception {
		if (valenciaItemType == null) {
			throw new Exception("ValenciaItemType is NULL!");
		}
		this.valenciaItemType = valenciaItemType;
		this.json = json;
		this.delayTime = delayTime;
	}

	@Override
	public void run(SourceContext<ValenciaItem> ctx) throws Exception {
		URL url = new URL(this.json);

		while (true) {
			InputStream is = url.openStream();
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
			StringBuilder builder = new StringBuilder();
			String line;

			try {
				while ((line = bufferedReader.readLine()) != null) {
					builder.append(line + "\n");
				}
				bufferedReader.close();

				ObjectMapper mapper = new ObjectMapper();
				JsonNode actualObj = mapper.readTree(builder.toString());
				List<Point> points = new ArrayList<Point>();

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
						if (valenciaItemType == ValenciaItemType.TRAFFIC) {
							for (JsonNode coordinates : arrayNodeCoordinates) {
								ArrayNode xy = (ArrayNode) coordinates;
								points.add(new Point(xy.get(0).asDouble(), xy.get(1).asDouble(), typeCSR));
							}
							valenciaItem = new ValenciaTraffic(null, null, "", new Date(), points,
									nodeProperties.get("estado").asInt());
						} else if (valenciaItemType == ValenciaItemType.AIR_POLLUTION) {
							String p = arrayNodeCoordinates.get(0).asText() + ","
									+ arrayNodeCoordinates.get(1).asText();
							points = Point.extract(p, typeCSR);
							valenciaItem = new ValenciaPollution(null, null, "", new Date(), points,
									nodeProperties.get("mediciones").asText());
						} else if (valenciaItemType == ValenciaItemType.NOISE) {
							throw new Exception("ValenciaItemType NOISE is not implemented!");
						} else {
							throw new Exception("ValenciaItemType is NULL!");
						}
						ctx.collect(valenciaItem);
					}
				}
			} catch (IOException ioe) {
				ioe.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				Thread.sleep(this.delayTime);
			}
		}
	}

	@Override
	public void cancel() {
	}
}

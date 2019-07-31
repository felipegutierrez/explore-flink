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
import org.sense.flink.pojo.ValenciaTraffic;

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
public class ValenciaTrafficJamConsumer extends RichSourceFunction<ValenciaTraffic> {

	private static final long serialVersionUID = 8320419468972434516L;
	private static final String VALENCIA_TRAFFIC_JAM_URL = "http://mapas.valencia.es/lanzadera/opendata/Tra-estado-trafico/JSON";
	private static final long DEFAULT_FREQUENCY_DELAY = 10000;
	private String json;
	private long delayTime;

	public ValenciaTrafficJamConsumer() {
		this(VALENCIA_TRAFFIC_JAM_URL, DEFAULT_FREQUENCY_DELAY);
	}

	public ValenciaTrafficJamConsumer(String json) {
		this(json, DEFAULT_FREQUENCY_DELAY);
	}

	public ValenciaTrafficJamConsumer(String json, long delayTime) {
		this.json = json;
		this.delayTime = delayTime;
	}

	@Override
	public void run(SourceContext<ValenciaTraffic> ctx) throws Exception {
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
					ValenciaTraffic valenciaTraffic = null;
					for (JsonNode jsonNode : arrayNodeFeatures) {
						JsonNode nodeProperties = jsonNode.get("properties");
						JsonNode nodeGeometry = jsonNode.get("geometry");
						ArrayNode arrayNodeCoordinates = (ArrayNode) nodeGeometry.get("coordinates");

						for (JsonNode coordinates : arrayNodeCoordinates) {
							ArrayNode xy = (ArrayNode) coordinates;
							points.add(new Point(xy.get(0).asDouble(), xy.get(1).asDouble(), typeCSR));
						}
						valenciaTraffic = new ValenciaTraffic(null, nodeProperties.get("denominacion").asText(),
								new Date(), nodeProperties.get("estado").asInt(), points);
						ctx.collect(valenciaTraffic);
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

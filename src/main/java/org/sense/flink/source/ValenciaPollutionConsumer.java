package org.sense.flink.source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.sense.flink.pojo.ValenciaPollution;

/**
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaPollutionConsumer extends RichSourceFunction<ValenciaPollution> {
	private static final long serialVersionUID = -2924802972034800231L;
	private static final String VALENCIA_POLLUTION_URL = "http://mapas.valencia.es/lanzadera/opendata/Estautomaticas/JSON";
	private String json;
	private long delayTime;

	public ValenciaPollutionConsumer() {
		this(VALENCIA_POLLUTION_URL, 10000);
	}

	public ValenciaPollutionConsumer(String json) {
		this(json, 10000);
	}

	public ValenciaPollutionConsumer(String json, long delayTime) {
		this.json = json;
		this.delayTime = delayTime;
	}

	@Override
	public void run(SourceContext<ValenciaPollution> ctx) throws Exception {
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

				boolean isCRS = actualObj.has("crs");
				boolean isFeatures = actualObj.has("features");
				String typeCSR = "";

				if (isCRS) {
					ObjectNode objectNodeCsr = (ObjectNode) actualObj.get("crs");
					ObjectNode objectNodeProperties = (ObjectNode) objectNodeCsr.get("properties");
					typeCSR = objectNodeProperties.get("name").asText();
					typeCSR = typeCSR.substring(typeCSR.indexOf("EPSG"));
				} else {
					System.out.println("Wrong CoordinateReferenceSystem (CSR) type");
				}

				if (isFeatures) {
					ArrayNode arrayNodeFeatures = (ArrayNode) actualObj.get("features");

					for (JsonNode jsonNode : arrayNodeFeatures) {
						JsonNode nodeProperties = jsonNode.get("properties");
						JsonNode nodeGeometry = jsonNode.get("geometry");
						ArrayNode arrayNodeCoordinates = (ArrayNode) nodeGeometry.get("coordinates");
						String p = arrayNodeCoordinates.get(0).asText() + "," + arrayNodeCoordinates.get(1).asText();

						ValenciaPollution valenciaPollution = new ValenciaPollution(
								nodeProperties.get("direccion").asText(), nodeProperties.get("mediciones").asText(),
								nodeProperties.get("mediciones").asText(), p, typeCSR);

						ctx.collect(valenciaPollution);
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

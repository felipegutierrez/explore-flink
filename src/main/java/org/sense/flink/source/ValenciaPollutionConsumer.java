package org.sense.flink.source;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaPollution;

/**
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaPollutionConsumer extends RichSourceFunction<ValenciaPollution> {
	private static final long serialVersionUID = -2924802972034800231L;
	public static final String VALENCIA_POLLUTION_URL = "http://mapas.valencia.es/lanzadera/opendata/Estautomaticas/JSON";
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

				boolean isResources = actualObj.has("features");
				if (isResources) {
					ArrayNode arrayNodeResources = (ArrayNode) actualObj.get("features");
					for (JsonNode jsonNode : arrayNodeResources) {

						JsonNode nodeProperties = jsonNode.get("properties");
						JsonNode nodeGeometry = jsonNode.get("geometry");
						ArrayNode coordinates = (ArrayNode) nodeGeometry.get("coordinates");
						Point p = new Point(coordinates.get(0).asDouble(), coordinates.get(1).asDouble());

						ValenciaPollution valenciaPollution = new ValenciaPollution(
								nodeProperties.get("direccion").asText(), nodeProperties.get("mediciones").asText(),
								nodeProperties.get("mediciones").asText(), p);

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

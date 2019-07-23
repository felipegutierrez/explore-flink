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
public class ValenciaNoiseConsumer extends RichSourceFunction<ValenciaPollution> {
	private static final long serialVersionUID = -2373602610753429092L;
	public static final String VALENCIA_NOISE_URL = "http://mapas.valencia.es/lanzadera/opendata/mapa_ruido/SHAPE";
	private String zipFile;
	private long delayTime;

	public ValenciaNoiseConsumer() {
		this(VALENCIA_NOISE_URL, 10000);
	}

	public ValenciaNoiseConsumer(String json) {
		this(json, 10000);
	}

	public ValenciaNoiseConsumer(String zipFile, long delayTime) {
		this.zipFile = zipFile;
		this.delayTime = delayTime;
	}

	@Override
	public void run(SourceContext<ValenciaPollution> ctx) throws Exception {
		URL url = new URL(this.zipFile);

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

				boolean isFeatures = actualObj.has("features");
				if (isFeatures) {
					ArrayNode arrayNodeFeatures = (ArrayNode) actualObj.get("features");
					for (JsonNode jsonNode : arrayNodeFeatures) {

						JsonNode nodeProperties = jsonNode.get("properties");
						JsonNode nodeGeometry = jsonNode.get("geometry");
						ArrayNode arrayNodeCoordinates = (ArrayNode) nodeGeometry.get("coordinates");
						Point p = new Point(arrayNodeCoordinates.get(0).asDouble(),
								arrayNodeCoordinates.get(1).asDouble());

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

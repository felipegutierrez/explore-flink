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
import org.sense.flink.pojo.ValenciaTraffic;

/**
 * @author Felipe Oliveira Gutierrez
 *
 */
public class ValenciaTrafficJamConsumer extends RichSourceFunction<ValenciaTraffic> {

	private static final long serialVersionUID = 8320419468972434516L;
	public static final String VALENCIA_TRAFFIC_JAM_URL = "http://apigobiernoabiertortod.valencia.es/apirtod/datos/estado_trafico.json";
	private String json;
	private long delayTime;

	public ValenciaTrafficJamConsumer() {
		this(VALENCIA_TRAFFIC_JAM_URL, 10000);
	}

	public ValenciaTrafficJamConsumer(String json) {
		this(json, 10000);
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

				// boolean isSummary = actualObj.has("summary"); // not used
				boolean isResources = actualObj.has("resources");
				if (isResources) {
					ArrayNode arrayNodeResources = (ArrayNode) actualObj.get("resources");
					for (JsonNode jsonNode : arrayNodeResources) {

						ValenciaTraffic valenciaTraffic = new ValenciaTraffic(jsonNode.get("idtramo").asInt(),
								jsonNode.get("denominacion").asText(), jsonNode.get("modified").asText(),
								jsonNode.get("estado").asInt(), jsonNode.get("coordinates").asText(), "EPSG:32630",
								jsonNode.get("uri").asText());

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

package org.sense.flink.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

public class RestAPIListening extends Thread {

	private URL url;
	private String host;
	HttpURLConnection conn;
	private int delay = 5000;
	private boolean running = false;

	public RestAPIListening(String host) {
		this.host = host;
		this.running = true;
	}

	public void connect() throws IOException {
		url = new URL(host);
	}

	public void run() {
		// BufferedReader in = null;
		try {
			while (running) {
				String readLine = null;
				int responseCode;

				conn = (HttpURLConnection) url.openConnection();
				conn.setRequestMethod("GET");
				responseCode = conn.getResponseCode();

				if (responseCode == HttpURLConnection.HTTP_OK) {
					BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
					StringBuffer response = new StringBuffer();
					while ((readLine = in.readLine()) != null) {
						response.append(readLine);
					}
					// print result
					// System.out.println("JSON String Result " + response.toString());
					ObjectMapper mapper = new ObjectMapper();
					ArrayNode arrayNodeMetrics = (ArrayNode) mapper.readTree(response.toString());
					for (JsonNode jsonNode : arrayNodeMetrics) {
						String id = jsonNode.get("id").asText();
						Double value = jsonNode.get("value").asDouble();
						System.out.println(id + ": " + value);
					}

				} else {
					System.out.println("GET NOT WORKED");
				}
				Thread.sleep(delay);
			}
			// in.close();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		String address = "http://127.0.0.1:8081/jobs/ee36f38988aea6ddf080da406bc956cb/vertices/6d2677a0ecc3fd8df0b72ec675edf8f4/metrics?get=0.Shuffle.Netty.Input.Buffers.outPoolUsage,1.Shuffle.Netty.Input.Buffers.outPoolUsage,2.Shuffle.Netty.Input.Buffers.outPoolUsage,3.Shuffle.Netty.Input.Buffers.outPoolUsage";
		RestAPIListening restAPIListening = new RestAPIListening(address);
		restAPIListening.connect();
		restAPIListening.start();
	}
}

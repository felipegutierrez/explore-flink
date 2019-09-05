package org.sense.flink.source;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
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
	// @formatter:off
	private static final long serialVersionUID = 8320419468972434516L;
	private static final String CURRENT_PATH = Paths.get("").toAbsolutePath().toString() + "/";
	private static final String RESOURCE_DIR = "resources/valencia/";
	private static final String VALENCIA_TRAFFIC_JAM_URL = "http://mapas.valencia.es/lanzadera/opendata/Tra-estado-trafico/JSON";
	private static final String VALENCIA_POLLUTION_URL = "http://mapas.valencia.es/lanzadera/opendata/Estautomaticas/JSON";
	private static final String VALENCIA_NOISE_URL = "http://???.??";
	private static final String VALENCIA_TRAFFIC_JAM_ONLINE_FILE = CURRENT_PATH + RESOURCE_DIR + "traffic-jam-state-online.json";
	private static final String VALENCIA_TRAFFIC_JAM_OFFLINE_FILE = CURRENT_PATH + RESOURCE_DIR + "traffic-jam-state-offline.json";
	private static final String VALENCIA_TRAFFIC_JAM_OFFLINE_SKEWED_FILE = CURRENT_PATH + RESOURCE_DIR + "traffic-jam-state-offline-skewed.json";
	private static final String VALENCIA_POLLUTION_ONLINE_FILE = CURRENT_PATH + RESOURCE_DIR + "air-pollution-online.json";
	private static final String VALENCIA_POLLUTION_OFFLINE_FILE = CURRENT_PATH + RESOURCE_DIR + "air-pollution-offline.json";
	private static final String VALENCIA_POLLUTION_OFFLINE_SKEWED_FILE = CURRENT_PATH + RESOURCE_DIR + "air-pollution-offline-skewed.json";
	private static final String VALENCIA_NOISE_FILE = CURRENT_PATH + RESOURCE_DIR + "noise.json";
	private static final String VALENCIA_NOISE_OFFLINE_FILE = CURRENT_PATH + RESOURCE_DIR + "noise-offline.json";
	private static final String VALENCIA_NOISE_OFFLINE_SKEWED_FILE = CURRENT_PATH + RESOURCE_DIR + "noise-offline-skewed.json";
	private static final long DEFAULT_INTERVAL_CHANGE_DATA_SOURCE = Time.minutes(5).toMilliseconds();

	private URL url;
	private long frequencyMilliSeconds;
	private long timeoutMillSeconds;
	private long startTime;
	private ValenciaItemType valenciaItemType;
	private boolean collectWithTimestamp;
	private boolean offlineData;
	private boolean dataSkewedSyntheticInjection;
	private boolean useDataSkewedFile;
	private volatile boolean running = true;
	// @formatter:on

	public ValenciaItemConsumer(ValenciaItemType valenciaItemType, long frequencyMilliSeconds,
			boolean collectWithTimestamp, boolean offlineData) throws Exception {
		this(valenciaItemType, frequencyMilliSeconds, collectWithTimestamp, offlineData, false);
	}

	/**
	 * 
	 * @param valenciaItemType
	 * @param frequencyMilliSeconds
	 * @param collectWithTimestamp
	 * @param offlineData
	 * @throws Exception
	 */
	public ValenciaItemConsumer(ValenciaItemType valenciaItemType, long frequencyMilliSeconds,
			boolean collectWithTimestamp, boolean offlineData, boolean dataSkewedSyntheticInjection) throws Exception {
		if (valenciaItemType == ValenciaItemType.TRAFFIC_JAM) {
			this.url = new URL(VALENCIA_TRAFFIC_JAM_URL);
			this.timeoutMillSeconds = Time.minutes(5).toMilliseconds();
		} else if (valenciaItemType == ValenciaItemType.AIR_POLLUTION) {
			this.url = new URL(VALENCIA_POLLUTION_URL);
			this.timeoutMillSeconds = Time.minutes(30).toMilliseconds();
		} else if (valenciaItemType == ValenciaItemType.NOISE) {
			this.url = new URL(VALENCIA_NOISE_URL);
			this.timeoutMillSeconds = Time.minutes(5).toMilliseconds();
		} else {
			throw new Exception("ValenciaItemType is NULL!");
		}
		this.offlineData = offlineData;
		this.valenciaItemType = valenciaItemType;
		this.frequencyMilliSeconds = frequencyMilliSeconds;
		this.collectWithTimestamp = collectWithTimestamp;
		this.dataSkewedSyntheticInjection = dataSkewedSyntheticInjection;
		this.useDataSkewedFile = false;
		this.startTime = Calendar.getInstance().getTimeInMillis();
		createResourceDir();
	}

	private void createResourceDir() {
		File directory = new File(CURRENT_PATH + RESOURCE_DIR);
		if (!directory.exists()) {
			directory.mkdir();
		}
	}

	@Override
	public void run(SourceContext<ValenciaItem> ctx) throws Exception {
		while (running) {
			// get the data source file to collect data
			File realTimeData = getDataSourceFile();

			boolean isNew = false;
			if (!offlineData) {
				if (realTimeData.exists()) {
					// check if the file is older than 5 minutes
					isNew = FileUtils.isFileNewer(realTimeData.getAbsoluteFile(),
							Calendar.getInstance().getTimeInMillis() - timeoutMillSeconds);
				} else {
					createResourceDir();
					// System.out.println("File [" + realTimeData.getAbsolutePath() + "] does not
					// exist!");
				}
				if (!isNew) {
					// System.out.println("File [" + realTimeData.getAbsolutePath() + "] does not
					// exist or it is old!");
					// System.out.println(realTimeData.getAbsoluteFile().toPath().toString());
					InputStream is = url.openStream();
					Files.copy(is, realTimeData.getAbsoluteFile().toPath(), StandardCopyOption.REPLACE_EXISTING);
				}
			}

			BufferedReader bufferedReader = Files.newBufferedReader(realTimeData.getAbsoluteFile().toPath());
			StringBuilder builder = new StringBuilder();
			String line;
			try {
				while ((line = bufferedReader.readLine()) != null) {
					builder.append(line + "\n");
				}
				bufferedReader.close();
				if (builder.length() == 0) {
					continue;
				}

				Date eventTime = new Date();
				ObjectMapper mapper = new ObjectMapper();
				JsonNode actualObj = mapper.readTree(builder.toString());

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
							String p = arrayNodeCoordinates.get(0).asText() + ","
									+ arrayNodeCoordinates.get(1).asText();
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
			} catch (IOException ioe) {
				ioe.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (useDataSkewedFile) {
					Thread.sleep(this.frequencyMilliSeconds / 1000);
				} else {
					Thread.sleep(this.frequencyMilliSeconds);
				}
			}
		}
	}

	private File getDataSourceFile() throws Exception {
		File realTimeData = null;
		// decide if we will inject the skewed data from an offline
		useDataSkewedFile = useDataSkewedFile();

		if (valenciaItemType == ValenciaItemType.TRAFFIC_JAM) {
			if (useDataSkewedFile) {
				realTimeData = new File(VALENCIA_TRAFFIC_JAM_OFFLINE_SKEWED_FILE);
			} else {
				if (offlineData) {
					realTimeData = new File(VALENCIA_TRAFFIC_JAM_OFFLINE_FILE);
				} else {
					realTimeData = new File(VALENCIA_TRAFFIC_JAM_ONLINE_FILE);
				}
			}
		} else if (valenciaItemType == ValenciaItemType.AIR_POLLUTION) {
			if (useDataSkewedFile) {
				realTimeData = new File(VALENCIA_POLLUTION_OFFLINE_SKEWED_FILE);
			} else {
				if (offlineData) {
					realTimeData = new File(VALENCIA_POLLUTION_OFFLINE_FILE);
				} else {
					realTimeData = new File(VALENCIA_POLLUTION_ONLINE_FILE);
				}
			}
		} else if (valenciaItemType == ValenciaItemType.NOISE) {
			if (useDataSkewedFile) {
				realTimeData = new File(VALENCIA_NOISE_OFFLINE_SKEWED_FILE);
			} else {
				if (offlineData) {
					realTimeData = new File(VALENCIA_NOISE_OFFLINE_FILE);
				} else {
					realTimeData = new File(VALENCIA_NOISE_FILE);
				}
			}
		} else {
			throw new Exception("ValenciaItemType is NULL!");
		}
		return realTimeData;
	}

	private boolean useDataSkewedFile() {
		if (dataSkewedSyntheticInjection) {
			long elapsedTime = Calendar.getInstance().getTimeInMillis() - DEFAULT_INTERVAL_CHANGE_DATA_SOURCE;
			if (elapsedTime >= startTime) {
				startTime = Calendar.getInstance().getTimeInMillis();
				useDataSkewedFile = (useDataSkewedFile ? false : true);

				String msg = "Changed source file. useDataSkewedFile[" + useDataSkewedFile + "] "
						+ valenciaItemType.toString();
				System.out.println(msg);
			}
		}
		return useDataSkewedFile;
	}

	@Override
	public void cancel() {
		running = false;
	}

	public static void main(String[] args) {
		System.out.println(Time.minutes(20).toMilliseconds());
		System.out.println((1000 * 60 * 20));
	}
}

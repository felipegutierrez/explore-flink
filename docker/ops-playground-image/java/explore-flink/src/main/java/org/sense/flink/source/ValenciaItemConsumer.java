package org.sense.flink.source;

import net.openhft.affinity.impl.LinuxJNAAffinity;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.sense.flink.pojo.Point;
import org.sense.flink.pojo.ValenciaItem;
import org.sense.flink.pojo.ValenciaPollution;
import org.sense.flink.pojo.ValenciaTraffic;
import org.sense.flink.util.CpuGauge;
import org.sense.flink.util.ValenciaItemType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;

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
 */
public class ValenciaItemConsumer extends RichSourceFunction<ValenciaItem> {
    // @formatter:off
    private static final long serialVersionUID = 8320419468972434516L;
    private static final String VALENCIA_TRAFFIC_JAM_URL = "http://mapas.valencia.es/lanzadera/opendata/Tra-estado-trafico/JSON";
    private static final String VALENCIA_POLLUTION_URL = "http://mapas.valencia.es/lanzadera/opendata/Estautomaticas/JSON";
    private static final String VALENCIA_NOISE_URL = "http://???.??";

    private static final String VALENCIA_TRAFFIC_JAM_ONLINE_FILE = "/valencia/traffic-jam-state-online.json";
    private static final String VALENCIA_TRAFFIC_JAM_OFFLINE_FILE = "/valencia/traffic-jam-state-offline.json";
    private static final String VALENCIA_TRAFFIC_JAM_OFFLINE_SKEWED_FILE = "/valencia/traffic-jam-state-offline-skewed.json";
    private static final String VALENCIA_POLLUTION_ONLINE_FILE = "/valencia/air-pollution-online.json";
    private static final String VALENCIA_POLLUTION_OFFLINE_FILE = "/valencia/air-pollution-offline.json";
    private static final String VALENCIA_POLLUTION_OFFLINE_SKEWED_FILE = "/valencia/air-pollution-offline-skewed.json";
    private static final String VALENCIA_NOISE_FILE = "/valencia/noise.json";
    private static final String VALENCIA_NOISE_OFFLINE_FILE = "/valencia/noise-offline.json";
    private static final String VALENCIA_NOISE_OFFLINE_SKEWED_FILE = "/valencia/noise-offline-skewed.json";
    private static final long DEFAULT_INTERVAL_CHANGE_DATA_SOURCE = Time.minutes(5).toMilliseconds();

    private final URL url;
    private final long frequencyMilliSeconds;
    private final long timeoutMillSeconds;
    private final long duration;
    private final long startTime;
    private final ValenciaItemType valenciaItemType;
    private final boolean collectWithTimestamp;
    private final boolean offlineData;
    private final boolean dataSkewedSyntheticInjection;
    private final int countMax;
    private long offsetTime;
    private boolean useDataSkewedFile;
    private boolean pinningPolicy = false;
    private volatile boolean running = true;
    private transient CpuGauge cpuGauge;
    private BitSet affinity;
    // @formatter:on

    public ValenciaItemConsumer(ValenciaItemType airPollution, long pollutionFrequency,
                                boolean collectWithTimestamp, boolean offlineData, boolean skewedDataInjection, long duration, boolean b) throws Exception {
        this(airPollution, pollutionFrequency, collectWithTimestamp, offlineData, false, Long.MAX_VALUE, false, -1);
    }

    public ValenciaItemConsumer(ValenciaItemType trafficJam, long toMilliseconds, boolean collectWithTimestamp, boolean offlineData) throws Exception {
        this(trafficJam, toMilliseconds, collectWithTimestamp, offlineData, false, Long.MAX_VALUE, false, -1);
    }

    public ValenciaItemConsumer(ValenciaItemType valenciaItemType, long frequencyMilliSeconds,
                                boolean collectWithTimestamp, boolean offlineData, int countMax) throws Exception {
        this(valenciaItemType, frequencyMilliSeconds, collectWithTimestamp, offlineData, false, Long.MAX_VALUE, false, countMax);
    }

    /**
     * @param valenciaItemType
     * @param frequencyMilliSeconds
     * @param collectWithTimestamp
     * @param offlineData
     * @throws Exception
     */
    public ValenciaItemConsumer(ValenciaItemType valenciaItemType, long frequencyMilliSeconds,
                                boolean collectWithTimestamp, boolean offlineData, boolean dataSkewedSyntheticInjection, long duration,
                                boolean pinningPolicy, int countMax) throws Exception {
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
        this.duration = Time.seconds(duration).toMilliseconds();
        this.startTime = Calendar.getInstance().getTimeInMillis();
        this.offsetTime = this.startTime;
        this.pinningPolicy = pinningPolicy;
        this.countMax = countMax;
    }

    public static void main(String[] args) {
        // System.out.println(Time.minutes(20).toMilliseconds());
        // System.out.println((1000 * 60 * 20));
        long max = Long.MAX_VALUE;
        if (max == Long.MAX_VALUE) {
            System.out.println("max == Long.MAX_VALUE");
        } else {
            System.out.println("max != Long.MAX_VALUE");
        }
    }

    @Override
    public void open(Configuration config) {
        this.cpuGauge = new CpuGauge();
        getRuntimeContext().getMetricGroup().gauge("cpu", cpuGauge);

        if (this.pinningPolicy) {
            // listing the cpu cores available
            int nbits = Runtime.getRuntime().availableProcessors();
            // pinning operator' thread to a specific cpu core
            this.affinity = new BitSet(nbits);
            affinity.set(((int) Thread.currentThread().getId() % nbits));
            LinuxJNAAffinity.INSTANCE.setAffinity(affinity);
        }
    }

    @Override
    public void run(SourceContext<ValenciaItem> ctx) throws Exception {
        int count = 0;
        while (running) {
            // updates the CPU core current in use
            this.cpuGauge.updateValue(LinuxJNAAffinity.INSTANCE.getCpu());
            // System.err.println(ValenciaItemConsumer.class.getSimpleName() + " thread[" + Thread.currentThread().getId() + "] core[" + this.cpuGauge.getValue() + "]");

            // get the data source file to collect data
            InputStream in = getDataSourceInputStream();

            if (!offlineData) {
                in = url.openStream();
            }

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
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
                                if (count % 10 == 0) {
                                    ctx.emitWatermark(new Watermark(eventTime.getTime()));
                                }
                            } else {
                                ctx.collect(valenciaItem);
                            }
                            count++;
                            if (count >= this.countMax) {
                                this.running = false;
                            }
                        }
                    }
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                Thread.sleep(this.frequencyMilliSeconds);
                this.checkDuration();
            }
        }
    }

    private InputStream getDataSourceInputStream() throws Exception {
        // decide if we will inject the skewed data from an offline
        useDataSkewedFile = useDataSkewedFile();

        if (valenciaItemType == ValenciaItemType.TRAFFIC_JAM) {
            if (useDataSkewedFile) {
                return getClass().getResourceAsStream(VALENCIA_TRAFFIC_JAM_OFFLINE_SKEWED_FILE);
            } else {
                if (offlineData) {
                    return getClass().getResourceAsStream(VALENCIA_TRAFFIC_JAM_OFFLINE_FILE);
                } else {
                    return getClass().getResourceAsStream(VALENCIA_TRAFFIC_JAM_ONLINE_FILE);
                }
            }
        } else if (valenciaItemType == ValenciaItemType.AIR_POLLUTION) {
            if (useDataSkewedFile) {
                return getClass().getResourceAsStream(VALENCIA_POLLUTION_OFFLINE_SKEWED_FILE);
            } else {
                if (offlineData) {
                    return getClass().getResourceAsStream(VALENCIA_POLLUTION_OFFLINE_FILE);
                } else {
                    return getClass().getResourceAsStream(VALENCIA_POLLUTION_ONLINE_FILE);
                }
            }
        } else if (valenciaItemType == ValenciaItemType.NOISE) {
            if (useDataSkewedFile) {
                return getClass().getResourceAsStream(VALENCIA_NOISE_OFFLINE_SKEWED_FILE);
            } else {
                if (offlineData) {
                    return getClass().getResourceAsStream(VALENCIA_NOISE_OFFLINE_FILE);
                } else {
                    return getClass().getResourceAsStream(VALENCIA_NOISE_FILE);
                }
            }
        } else {
            throw new Exception("ValenciaItemType is NULL!");
        }
    }

    private boolean useDataSkewedFile() {
        if (dataSkewedSyntheticInjection) {
            long elapsedTime = Calendar.getInstance().getTimeInMillis() - DEFAULT_INTERVAL_CHANGE_DATA_SOURCE;
            if (elapsedTime >= this.offsetTime) {
                this.offsetTime = Calendar.getInstance().getTimeInMillis();
                useDataSkewedFile = (!useDataSkewedFile);

                String msg = "Changed source file. useDataSkewedFile[" + useDataSkewedFile + "] "
                        + valenciaItemType.toString();
                System.out.println(msg);
            }
        }
        return useDataSkewedFile;
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    private void checkDuration() {
        if (this.duration != Long.MAX_VALUE) {
            long elapsedTime = Calendar.getInstance().getTimeInMillis() - this.startTime;
            if (elapsedTime >= this.duration) {
                this.cancel();
            }
        }
    }
}

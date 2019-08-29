package org.sense.flink;

import org.apache.flink.runtime.client.JobExecutionException;
import org.sense.flink.examples.batch.MatrixMultiplication;
import org.sense.flink.examples.stream.edgent.AdaptiveFilterRangeMqttEdgent;
import org.sense.flink.examples.stream.edgent.MqttSensorDataCombinerByKeySkewedDAG;
import org.sense.flink.examples.stream.edgent.MqttSensorDataHLLKeyedProcessWindow;
import org.sense.flink.examples.stream.edgent.MqttSensorDataSkewedCombinerByKeySkewedDAG;
import org.sense.flink.examples.stream.edgent.MqttSensorDataSkewedJoinDAG;
import org.sense.flink.examples.stream.edgent.MqttSensorDataSkewedPartitionByKeyDAG;
import org.sense.flink.examples.stream.edgent.MqttSensorDataSkewedPartitionByKeySkewedDAG;
import org.sense.flink.examples.stream.edgent.MqttSensorDataSkewedRescaleByKeyDAG;
import org.sense.flink.examples.stream.edgent.MultiSensorMultiStationsJoinMqtt;
import org.sense.flink.examples.stream.edgent.MultiSensorMultiStationsReadingMqtt;
import org.sense.flink.examples.stream.edgent.MultiSensorMultiStationsReadingMqtt2;
import org.sense.flink.examples.stream.edgent.SensorsDynamicFilterMqttEdgentQEP;
import org.sense.flink.examples.stream.edgent.SensorsMultipleReadingMqttEdgentQEP;
import org.sense.flink.examples.stream.edgent.SensorsMultipleReadingMqttEdgentQEP2;
import org.sense.flink.examples.stream.edgent.SensorsReadingMqttEdgentQEP;
import org.sense.flink.examples.stream.edgent.SensorsReadingMqttJoinQEP;
import org.sense.flink.examples.stream.edgent.TemperatureAverageExample;
import org.sense.flink.examples.stream.edgent.WordCountMqttFilterQEP;
import org.sense.flink.examples.stream.edgent.WordCountSocketFilterQEP;
import org.sense.flink.examples.stream.table.MqttSensorDataAverageTableAPI;
import org.sense.flink.examples.stream.twitter.TwitterExample;
import org.sense.flink.examples.stream.valencia.ValenciaDataCpuIntensiveJoinExample;
import org.sense.flink.examples.stream.valencia.ValenciaDataSkewedBloomFilterJoinExample;
import org.sense.flink.examples.stream.valencia.ValenciaDataSkewedBroadcastJoinExample;
import org.sense.flink.examples.stream.valencia.ValenciaDataSkewedCombinerExample;
import org.sense.flink.examples.stream.valencia.ValenciaDataSkewedJoinExample;
import org.sense.flink.examples.stream.valencia.ValenciaDataSkewedRepartitionJoinExample;

/**
 * 
 * <pre>
 * parameters to run this application:
 * -app 30 -source 127.0.0.1 -sink 127.0.0.1 -offlineData true -frequencyPull 10 -frequencyWindow 30 -syntheticData true
 * </pre>
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class App {
	private static String PARAMETER_APP = "-app";
	private static String PARAMETER_SOURCE = "-source";
	private static String PARAMETER_SINK = "-sink";
	private static String PARAMETER_OFFLINE_DATA = "-offlineData";
	private static String PARAMETER_FREQUENCY_PULL = "-frequencyPull";
	private static String PARAMETER_FREQUENCY_WINDOW = "-frequencyWindow";
	private static String PARAMETER_SYNTHETIC_DATA = "-syntheticData";

	public static void main(String[] args) throws Exception {

		listApplications();

		int app = 0;
		String ipAddressSource = "127.0.0.1";
		String ipAddressSink = "127.0.0.1";
		int frequencyPull = 10;
		int frequencyWindow = 30;
		boolean offlinedata = false;
		boolean syntheticData = false;

		if (args != null && args.length > 0) {
			int size = args.length;
			for (int i = 0; i < size; i++) {
				if (PARAMETER_APP.equals(String.valueOf(args[i])) && i + 1 < size) {
					i++;
					app = Integer.parseInt(args[i]);
				} else if (PARAMETER_SOURCE.equals(String.valueOf(args[i])) && i + 1 < size) {
					i++;
					ipAddressSource = String.valueOf(args[i]);
					if (!validIP(ipAddressSource)) {
						System.err.println("IP address invalid[" + ipAddressSource + "]");
					}
				} else if (PARAMETER_SINK.equals(String.valueOf(args[i])) && i + 1 < size) {
					i++;
					ipAddressSink = String.valueOf(args[i]);
					if (!validIP(ipAddressSink)) {
						System.err.println("IP address invalid[" + ipAddressSink + "]");
					}
				} else if (PARAMETER_OFFLINE_DATA.equals(String.valueOf(args[i])) && i + 1 < size) {
					i++;
					offlinedata = Boolean.valueOf(args[i]);
				} else if (PARAMETER_FREQUENCY_PULL.equals(String.valueOf(args[i])) && i + 1 < size) {
					i++;
					frequencyPull = Integer.parseInt(args[i]);
				} else if (PARAMETER_FREQUENCY_WINDOW.equals(String.valueOf(args[i])) && i + 1 < size) {
					i++;
					frequencyWindow = Integer.parseInt(args[i]);
				} else if (PARAMETER_SYNTHETIC_DATA.equals(String.valueOf(args[i])) && i + 1 < size) {
					i++;
					syntheticData = Boolean.valueOf(args[i]);
				}
			}
		}
		System.out.println();
		System.out.println("Application selected: " + app);
		System.out.println("ipAddressSource: " + ipAddressSource);
		System.out.println("ipAddressSink: " + ipAddressSink);
		System.out.println("offlinedata: " + offlinedata);
		System.out.println("frequencyPull: " + frequencyPull);
		System.out.println("frequencyWindow: " + frequencyWindow);
		System.out.println("syntheticData: " + syntheticData);

		try {
			switch (app) {
			case 0:
				System.out.println("bis später");
				break;
			case 1:
				System.out.println("App 1 selected");
				new WordCountSocketFilterQEP();
				app = 0;
				break;
			case 2:
				System.out.println("App 2 selected");
				new WordCountMqttFilterQEP();
				app = 0;
				break;
			case 3:
				System.out.println("App 3 selected");
				new MatrixMultiplication();
				app = 0;
				break;
			case 4:
				System.out.println("App 4 selected");
				new SensorsReadingMqttJoinQEP();
				app = 0;
				break;
			case 5:
				System.out.println("App 5 selected");
				new SensorsReadingMqttEdgentQEP();
				app = 0;
				break;
			case 6:
				System.out.println("App 6 selected");
				System.out.println("use 'mosquitto_pub -h 127.0.0.1 -t topic-parameter -m \"0.0,1000.0\"' ");
				System.out.println("on the terminal to change the parameters at runtime.");
				new SensorsDynamicFilterMqttEdgentQEP();
				app = 0;
				break;
			case 7:
				System.out.println("App 7 selected");
				System.out.println("The adaptive filter was pushed down to the data source");
				new AdaptiveFilterRangeMqttEdgent();
				app = 0;
				break;
			case 8:
				System.out.println("App 8 selected");
				System.out.println("Temperature average example");
				new TemperatureAverageExample();
				app = 0;
				break;
			case 9:
				// @formatter:off
					System.out.println("App 9 selected");
					System.out.println("Use [./bin/flink run examples/explore-flink.jar 9 -c] to run this program on the Flink standalone-cluster");
					System.out.println("Consuming values from 3 MQTT topics");
					// @formatter:on
				new SensorsMultipleReadingMqttEdgentQEP();
				app = 0;
				break;
			case 10:
				// @formatter:off
					System.out.println("App 10 selected");
					System.out.println("Use [./bin/flink run examples/explore-flink.jar 10 -c] to run this program on the Flink standalone-cluster");
					System.out.println("Consuming values from 3 MQTT topics");
					// @formatter:on
				new SensorsMultipleReadingMqttEdgentQEP2();
				app = 0;
				break;
			case 11:
				// @formatter:off
					System.out.println("App 11 selected (ValueState)");
					System.out.println("Use [./bin/flink run examples/explore-flink.jar 11 -c] to run this program on the Flink standalone-cluster");
					System.out.println("Consuming values from 2 MQTT topics");
					// @formatter:on
				new MultiSensorMultiStationsReadingMqtt();
				app = 0;
				break;
			case 12:
				// @formatter:off
					System.out.println("App 12 selected (window)");
					System.out.println("Use [./bin/flink run examples/explore-flink.jar 12 -c] to run this program on the Flink standalone-cluster");
					System.out.println("Consuming values from 2 MQTT topics");
					// @formatter:on
				new MultiSensorMultiStationsReadingMqtt2();
				app = 0;
				break;
			case 13:
				// @formatter:off
					System.out.println("App 13 selected (join within a window)");
					System.out.println("Use [./bin/flink run examples/explore-flink.jar 13 -c] to run this program on the Flink standalone-cluster");
					System.out.println("Consuming values from 2 MQTT topics");
					// @formatter:on
				new MultiSensorMultiStationsJoinMqtt();
				app = 0;
				break;
			case 14:
				new MqttSensorDataSkewedJoinDAG(ipAddressSource, ipAddressSink);
				app = 0;
				break;
			case 15:
				new MqttSensorDataSkewedPartitionByKeyDAG(ipAddressSource, ipAddressSink);
				app = 0;
				break;
			case 16:
				new MqttSensorDataSkewedRescaleByKeyDAG(ipAddressSource, ipAddressSink);
				app = 0;
				break;
			case 17:
				new MqttSensorDataSkewedPartitionByKeySkewedDAG(ipAddressSource, ipAddressSink);
				app = 0;
				break;
			case 18:
				new MqttSensorDataSkewedCombinerByKeySkewedDAG(ipAddressSource, ipAddressSink);
				app = 0;
				break;
			case 19:
				new MqttSensorDataCombinerByKeySkewedDAG(ipAddressSource, ipAddressSink);
				app = 0;
				break;
			case 20:
				System.out.println("application 20");
				new TwitterExample(args);
				app = 0;
				break;
			case 21:
				System.out.println("Estimate cardinality with HyperLogLog");
				new MqttSensorDataHLLKeyedProcessWindow(ipAddressSource, ipAddressSink);
				app = 0;
				break;
			case 24:
				new MqttSensorDataAverageTableAPI(ipAddressSource, ipAddressSink);
				app = 0;
				break;
			case 25:
				new ValenciaDataSkewedJoinExample();
				app = 0;
				break;
			case 26:
				new ValenciaDataSkewedCombinerExample(ipAddressSource, ipAddressSink);
				app = 0;
				break;
			case 27:
				new ValenciaDataSkewedRepartitionJoinExample(ipAddressSource, ipAddressSink);
				app = 0;
				break;
			case 28:
				new ValenciaDataSkewedBroadcastJoinExample(ipAddressSource, ipAddressSink);
				app = 0;
				break;
			case 29:
				new ValenciaDataSkewedBloomFilterJoinExample(ipAddressSource, ipAddressSink);
				app = 0;
				break;
			case 30:
				new ValenciaDataCpuIntensiveJoinExample(ipAddressSource, ipAddressSink, offlinedata, frequencyPull,
						frequencyWindow);
				app = 0;
				break;
			default:
				args = null;
				System.out.println("No application selected [" + app + "] ");
				break;
			}
		} catch (JobExecutionException ce) {
			System.err.println(ce.getMessage());
			ce.printStackTrace();
		}
	}

	private static void listApplications() {
		// @formatter:off
		System.out.println("0  - exit");
		System.out.println("1  - World count (Sokect stream) with Filter and QEP");
		System.out.println("2  - World count (MQTT stream) with Filter and QEP");
		System.out.println("3  - Matrix multiplication using batch and QEP");
		System.out.println("4  - Two fake sensors (MQTT stream) and QEP");
		System.out.println("5  - Fake sensor from Apache Edgent (MQTT stream) and QEP");
		System.out.println("6  - Dynamic filter over fake data source and QEP");
		System.out.println("7  - Adaptive filter pushed down to Apache Edgent");
		System.out.println("8  - Temperature average example");
		System.out.println("9  - Consume MQTT from multiple temperature sensors");
		System.out.println("10 - Consume MQTT from multiple temperature sensors");
		System.out.println("11 - Consume MQTT from multiple sensors at train stations with ValueState");
		System.out.println("12 - Consume MQTT from multiple sensors at train stations with window");
		System.out.println("13 - Consume MQTT from multiple sensors at train stations and join within a window");
		System.out.println("14 - Consume MQTT from multiple sensors at train stations and join within a window");
		System.out.println("15 - Partition by Key and Reducing over a window");
		System.out.println("16 - Custom Partition by Key and Reducing over a window");
		System.out.println("17 - Random Partition by Key and Reducing over a window");
		System.out.println("18 - Combiner on the map phase just before shuffling and Reducing over a window");
		System.out.println("19 - Combiner on the map phase just before shuffling and Reducing over a window with RichFunction");
		System.out.println("20 - TwitterExample");
		System.out.println("21 - Estimate cardinality with HyperLogLog");
		System.out.println("22 - Estimate cardinality with HyperLogLogPlus");
		System.out.println("23 - Estimate cardinality with Bloom Filter");
		System.out.println("24 - Executin join over MQTT data using Flink Table API");
		System.out.println("25 - Reading values from Valencia Open-data Web Portal and processing a JOIN using Flink Data Stream");
		System.out.println("26 - Reading values from Valencia Open-data Web Portal and processing a COMBINER using Flink Data Stream");
		System.out.println("27 - Reading values from Valencia Open-data Web Portal and computing the Standard Repartition JOIN using Flink Data Stream");
		System.out.println("28 - Reading values from Valencia Open-data Web Portal and computing the Broadcast JOIN using Flink Data Stream");
		System.out.println("29 - Reading values from Valencia Open-data Web Portal and computing the Improved Repartition JOIN with Bloom Filter using Flink Data Stream");
		System.out.println("30 - Reading values from Valencia Open-data Web Portal and CPU intensive computation using Flink Data Stream");
		// @formatter:on
	}

	public static boolean validIP(String ip) {
		try {
			if (ip == null || ip.isEmpty()) {
				return false;
			}

			String[] parts = ip.split("\\.");
			if (parts.length != 4) {
				return false;
			}

			for (String s : parts) {
				int i = Integer.parseInt(s);
				if ((i < 0) || (i > 255)) {
					return false;
				}
			}
			if (ip.endsWith(".")) {
				return false;
			}

			return true;
		} catch (NumberFormatException nfe) {
			return false;
		}
	}
}

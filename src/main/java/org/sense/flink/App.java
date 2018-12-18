package org.sense.flink;

import java.util.Scanner;

import org.apache.flink.runtime.client.JobExecutionException;
import org.sense.flink.examples.batch.MatrixMultiplication;
import org.sense.flink.examples.stream.SensorsReadingMqttJoinQEP;
import org.sense.flink.examples.stream.WordCountMqttFilterQEP;
import org.sense.flink.examples.stream.WordCountSocketFilterQEP;

/**
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class App {
	public static void main(String[] args) throws Exception {

		try {
			int app = 0;
			do {
				System.out.println("0 - exit");
				System.out.println(
						"1 - World count (Sokect stream) with Filter and showing the Query Execution Plan (QEP)");
				System.out.println(
						"2 - World count (MQTT stream) with Filter and showing the Query Execution Plan (QEP)");
				System.out.println("3 - Matrix multiplication using batch");
				System.out.println("4 - Join Data Sources example using Data Stream (QEP)");
				System.out.print("    Please enter which application you want to run: ");

				String msg = (new Scanner(System.in)).nextLine();
				app = Integer.valueOf(msg);
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
					app = 0;
					break;
				default:
					System.out.println("No application selected [" + app + "] ");
					break;
				}
			} while (app != 0);
		} catch (JobExecutionException ce) {
			System.err.println(ce.getMessage());
			ce.printStackTrace();
		}
	}
}

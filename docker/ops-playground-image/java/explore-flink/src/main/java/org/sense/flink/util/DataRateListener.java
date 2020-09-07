package org.sense.flink.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public class DataRateListener extends Thread implements Serializable {
	private static final long serialVersionUID = 1L;
	public static final String DATA_RATE_FILE = "/tmp/datarate.txt";
	private long delayInNanoSeconds;
	private boolean running;

	public DataRateListener() {
		// 1 millisecond = 1.000.000 nanoseconds
		// 1.000.000.000 = 1 second
		// 1.000.000.000 / 1.000.000 = 1.000 records/second
		this.delayInNanoSeconds = 1000000000;
		this.running = true;
		this.disclaimer();
	}

	public static void main(String[] args) {
		DataRateListener drl = new DataRateListener();
		long start;
		System.out.println("delay                        : " + drl.delayInNanoSeconds);
		for (int i = 0; i < 100; i++) {
			start = System.nanoTime();
			System.out.print("start : " + start);
			drl.busySleep(start);
			System.out.println(" finish: " + (System.nanoTime() - start));
		}
	}

	private void disclaimer() {
		System.out.println(DataRateListener.class.getSimpleName() + " class to read data rate from file ["
				+ DATA_RATE_FILE + "] in milliseconds.");
		System.out.println("This listener reads every 60 seconds only the first line from the data rate file.");
		System.out.println("Use the following command to change the millisecond data rate:");
		System.out.println("echo \"1000000\" > " + DATA_RATE_FILE);
		System.out.println();
	}

	public void run() {
		while (running) {
			File fileName = new File(DATA_RATE_FILE);
			if (!fileName.exists()) {
				System.err.println("The file [" + DATA_RATE_FILE
						+ "] is not available and the application is using the default rate in nanoseconds ["
						+ this.delayInNanoSeconds + "]. This is: " + (this.delayInNanoSeconds / 1000000)
						+ " records/second");
			}
			try (BufferedReader br = new BufferedReader(
					new InputStreamReader(new FileInputStream(fileName), StandardCharsets.UTF_8))) {
				String line;
				while ((line = br.readLine()) != null) {
					// System.out.println(line);
					if (isNumeric(line)) {
						if (Long.parseLong(line) > 0) {
							System.out.println("Reading new frequency to generate data: " + line + " nanoseconds.");
							delayInNanoSeconds = Long.parseLong(line);
						} else {
							System.out.println("ERROR: new frequency must be greater or equal to 1.");
						}
					} else if ("SHUTDOWN".equalsIgnoreCase(line)) {
						running = false;
					} else {
						System.out.println(line);
					}
				}

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				Thread.sleep(60 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public long getDelayInNanoSeconds() {
		return this.delayInNanoSeconds;
	}

	public void busySleep(long startTime) {
		long deadLine = startTime + this.delayInNanoSeconds;
		while (System.nanoTime() < deadLine)
			;
	}

	public boolean isNumeric(final String str) {
		// null or empty
		if (str == null || str.length() == 0) {
			return false;
		}
		for (char c : str.toCharArray()) {
			if (!Character.isDigit(c)) {
				return false;
			}
		}
		return true;
	}
}

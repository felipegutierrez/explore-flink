package org.sense.flink.util;

import java.lang.instrument.Instrumentation;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * The camera has a range to move, so it is generating images according to that
 * range.
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class CameraSensor implements ISensor {

	private static Instrumentation instrumentation;
	private double latitude;
	private double longitude;
	private double altitude;
	private ByteBuffer buf;

	private double latitudeMin = 52.52 - 1;
	private double latitudeMax = 52.52 + 1;
	private boolean direction = true;

	public CameraSensor() {
		this.latitude = 52.5; // ~52.520008
		this.longitude = 13.40; // ~13.404954
		this.altitude = 34; // ~34 meters
	}

	@Override
	public byte[] readRequest() {
		// generate random location value

		if (direction) {
			this.latitude = this.latitude + 0.1;
		} else {
			this.latitude = this.latitude - 0.1;
		}

		if (this.latitude <= this.latitudeMin) {
			this.direction = true;
		} else if (this.latitude >= this.latitudeMax) {
			this.direction = false;
		}

		int rangeMin = -10;
		int rangeMax = 50;
		Random r = new Random();
		double randomValue = rangeMin + (rangeMax - rangeMin) * r.nextDouble();

		int latitudeSize = 8;
		int longitudeSize = 8;
		int altitudeSize = 8;
		int valueSize = 8;

		byte[] bytes = new byte[latitudeSize + longitudeSize + altitudeSize + valueSize];
		buf = ByteBuffer.wrap(bytes);
		buf.putDouble(this.latitude).putDouble(this.longitude).putDouble(this.altitude).putDouble(randomValue);
		return buf.array();
	}

	public void printReadRequest() {
		// Bytes to doubles
		int times = Double.SIZE / Byte.SIZE;
		double[] doubles = new double[buf.array().length / times];
		for (int i = 0; i < doubles.length; i++) {
			doubles[i] = ByteBuffer.wrap(buf.array(), i * times, times).getDouble();
		}
		System.out.println("Location[Lat " + doubles[0] + ", Lon " + doubles[1] + ", Alt " + doubles[2]
				+ "] video recorded: " + doubles[3]);
	}
}

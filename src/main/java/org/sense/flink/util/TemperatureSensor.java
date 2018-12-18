package org.sense.flink.util;

import java.nio.ByteBuffer;
import java.util.Random;

public class TemperatureSensor implements ISensor {

	private double latitude;
	private double longitude;
	private double altitude;
	private ByteBuffer buf;

	public TemperatureSensor() {
		this.latitude = 52.5; // ~52.520008
		this.longitude = 13.40; // ~13.404954
		this.altitude = 34; // ~34 meters
	}

	public TemperatureSensor(double latitude, double longitude, double altitude, ByteBuffer buf) {
		this.latitude = latitude;
		this.longitude = longitude;
		this.altitude = altitude;
		this.buf = buf;
	}

	@Override
	public byte[] readRequest() {

		// generate random temperature value
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
				+ "] temperature: " + doubles[3]);
	}

	@Override
	public String toString() {
		return "TemperatureSensor [latitude=" + latitude + ", longitude=" + longitude + ", altitude=" + altitude
				+ ", buf=" + ByteBuffer.wrap(buf.array()).getDouble() + "]";
	}
}

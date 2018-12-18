package org.sense.flink.sensor;

import java.nio.ByteBuffer;

/**
 * The camera has a range to move, so it is generating images according to that
 * range.
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class CameraSnapshotSensor implements ISensor {

	private double latitude;
	private double longitude;
	private double altitude;
	private byte[] snapshot;
	private byte[] temperature;

	public CameraSnapshotSensor() {
	}

	public CameraSnapshotSensor(double latitude, double longitude, double altitude, byte[] snapshot,
			byte[] temperature) {
		this.latitude = latitude;
		this.longitude = longitude;
		this.altitude = altitude;
		this.snapshot = snapshot;
		this.temperature = temperature;
	}

	@Override
	public byte[] readRequest() {
		int latitudeSize = 8;
		int longitudeSize = 8;
		int altitudeSize = 8;
		int snapshotSize = 8;
		int temperatureSize = 8;

		byte[] bytes = new byte[latitudeSize + longitudeSize + altitudeSize + snapshotSize + temperatureSize];
		return null;
	}

	public void printReadRequest() {
		// Bytes to doubles
	}

	@Override
	public String toString() {
		return "CameraSnapshotSensor [latitude=" + latitude + ", longitude=" + longitude + ", altitude=" + altitude
				+ ", snapshot=" + ByteBuffer.wrap(snapshot).getDouble() + ", temperature="
				+ ByteBuffer.wrap(temperature).getDouble() + "]";
	}
}

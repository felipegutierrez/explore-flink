package org.sense.flink.sensor;

public interface ISensor {

	public byte[] readRequest();

	public void printReadRequest();
}

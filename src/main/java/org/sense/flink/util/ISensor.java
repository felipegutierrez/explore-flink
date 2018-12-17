package org.sense.flink.util;

public interface ISensor {

	public byte[] readRequest();

	public void printReadRequest();
}

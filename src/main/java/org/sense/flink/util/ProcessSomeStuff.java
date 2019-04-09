package org.sense.flink.util;

import org.sense.flink.mqtt.CompositeKeyStationPlatform;

public class ProcessSomeStuff {

	public static void processSomeStuff(CompositeKeyStationPlatform compositeKey, int delay)
			throws InterruptedException {
		if (compositeKey.getStationId().equals(new Integer(2)) && compositeKey.getPlatformId().equals(new Integer(3))) {
			System.out.println("processing some stuff for key[" + compositeKey + "]");
			Thread.sleep(delay);
		}
	}
}

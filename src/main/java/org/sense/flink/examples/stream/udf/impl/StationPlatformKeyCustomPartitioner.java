package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.functions.Partitioner;
import org.sense.flink.mqtt.CompositeKeyStationPlatform;

public class StationPlatformKeyCustomPartitioner implements Partitioner<CompositeKeyStationPlatform> {
	private static final long serialVersionUID = 7975348053137247432L;

	@Override
	public int partition(CompositeKeyStationPlatform key, int numPartitions) {
		int partition = (key.getStationId() + key.getPlatformId()) % numPartitions;
		System.out.println("numPartitions[" + numPartitions + "]: " + partition);
		return partition;
	}
}

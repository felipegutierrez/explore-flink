package org.sense.flink.util;

public enum SensorTopics {
	TOPIC_STATION_01_PLAT_01_TICKETS("topic-station-01-plat-01-tickets"),
	TOPIC_STATION_01_PLAT_02_TICKETS("topic-station-01-plat-02-tickets"),
	TOPIC_STATION_01_PLAT_01_TICKETS_CARDINALITY("topic-station-01-plat-01-tickets-car"),
	TOPIC_STATION_01_PLAT_02_TICKETS_CARDINALITY("topic-station-01-plat-02-tickets-car");

	private String value;

	SensorTopics(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}

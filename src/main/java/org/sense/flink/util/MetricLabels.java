package org.sense.flink.util;

public class MetricLabels {
	/** Metrics for virtual sensors using Apache Edgent */
	public final static String METRIC_SENSOR_MAPPER = "SensorTypeStationPlatformMapper";
	public final static String METRIC_SENSOR_COMBINER = "SensorTypeStationPlatformCombiner";
	public final static String METRIC_SENSOR_STATION_PLATFORM_MAPPER = "StationPlatformMapper";
	public final static String METRIC_SENSOR_STATION_PLATFORM_WINDOW = "StationPlatformRichWindow";
	public final static String METRIC_SENSOR_STATION_PLATFORM_SKEWED_MAPPER = "StationPlatformSkewedMapper";
	public final static String METRIC_SENSOR_SINK = "SensorSink";

	/** Metrics for real sensors form Valencia open-data web portal */
	public final static String METRIC_VALENCIA_COMBINER = "ValenciaDistrictCombiner";

}

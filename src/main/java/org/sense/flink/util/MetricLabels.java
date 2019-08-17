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
	public final static String METRIC_VALENCIA_WINDOW = "ValenciaDistrictWindow";
	public final static String METRIC_VALENCIA_FILTER = "ValenciaDistrictFilter";
	public final static String METRIC_VALENCIA_DISTRICT_MAP = "ValenciaDistrictMap";
	public final static String METRIC_VALENCIA_POLLUTION_MAP = "ValenciaPollutionMap";
	public final static String METRIC_VALENCIA_TRAFFIC_MAP = "ValenciaTrafficMap";
	public final static String METRIC_VALENCIA_DISTRICT_KEY_MAP = "ValenciaDistricKeyMap";
	public final static String METRIC_VALENCIA_STRING_MAP = "ValenciaStringMap";
	public final static String METRIC_VALENCIA_SINK = "ValenciaSink";
	public final static String METRIC_VALENCIA_SYNTHETIC_FLATMAP = "ValenciaSyntheticFlatMap";
	public final static String METRIC_VALENCIA_SIDE_OUTPUT = "ValenciaSideOutput";
	public final static String METRIC_VALENCIA_SOURCE = "ValenciaSource";
	public final static String METRIC_VALENCIA_JOIN = "ValenciaDistrictJoin";
}

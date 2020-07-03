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
	public final static String METRIC_VALENCIA_FILTERED = "ValenciaFiltered";
	public final static String METRIC_VALENCIA_LOOKUP = "ValenciaLookup";
	public final static String METRIC_VALENCIA_SOURCE = "ValenciaSource";
	public final static String METRIC_VALENCIA_FREQUENCY_PARAMETER_SOURCE = "ValenciaFrequencyPullParameterSource";
	public final static String METRIC_VALENCIA_FREQUENCY_PARAMETER = "ValenciaFrequencyPullParameter";
	public final static String METRIC_VALENCIA_JOIN = "ValenciaDistrictJoin";
	public final static String METRIC_VALENCIA_WATERMARKER_ASSIGNER = "ValenciaWatermarkerAssigner";
	public final static String METRIC_VALENCIA_CPU_INTENSIVE_MAP = "ValenciaIntensiveCpu";

	
	/** operator names and other parameters for the TPC-H Benchmark */
	public static final String OPERATOR_SOURCE = "source";
	public static final String OPERATOR_TOKENIZER = "tokenizer";
	public static final String OPERATOR_REDUCER = "reducer";
	public static final String OPERATOR_AGGREGATE = "aggregate";
	public static final String OPERATOR_PRE_AGGREGATE = "pre-aggregate";
	public static final String OPERATOR_FLAT_OUTPUT = "flat-output";
	public static final String OPERATOR_SINK = "sink";
	public static final String TPCH_DATA_LINE_ITEM = "/home/flink/tpch-dbgen/data/lineitem.tbl";
	public static final String TPCH_DATA_ORDER = "/home/flink/tpch-dbgen/data/orders.tbl";
	public static final String TPCH_DATA_COSTUMER = "/home/flink/tpch-dbgen/data/customer.tbl";
	public static final String TPCH_DATA_NATION = "/home/flink/tpch-dbgen/data/nation.tbl";
	public static final String ROCKSDB_STATE_DIR_NFS = "file:///nfshome/flink/rocsdb-flink-state";
	public static final String SINK_HOST = "sinkHost";
	public static final String SINK_PORT = "sinkPort";
	public static final String SINK = "output";
	public static final String TOPIC_DATA_SINK = "topic-data-sink";
}

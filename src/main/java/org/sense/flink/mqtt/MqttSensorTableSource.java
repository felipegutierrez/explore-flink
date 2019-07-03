package org.sense.flink.mqtt;

import static org.sense.flink.util.SensorColumn.EVENTTIME;
import static org.sense.flink.util.SensorColumn.PLATFORM_ID;
import static org.sense.flink.util.SensorColumn.PLATFORM_TYPE;
import static org.sense.flink.util.SensorColumn.SENSOR_ID;
import static org.sense.flink.util.SensorColumn.SENSOR_TYPE;
import static org.sense.flink.util.SensorColumn.STATION_ID;
import static org.sense.flink.util.SensorColumn.TIMESTAMP;
import static org.sense.flink.util.SensorColumn.TRIP;
import static org.sense.flink.util.SensorColumn.VALUE;

import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.StreamRecordTimestamp;
import org.apache.flink.table.sources.wmstrategies.PreserveWatermarks;
import org.apache.flink.types.Row;

public class MqttSensorTableSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {
	private final String topic;
	private final String ipAddressSource;
	private final String flag;

	public MqttSensorTableSource() {
		this("127.0.0.1", "topic-station-01-people", "");
	}

	public MqttSensorTableSource(String ipAddressSource, String topic) {
		this(ipAddressSource, topic, "");
	}

	public MqttSensorTableSource(String ipAddressSource, String topic, String flag) {
		this.ipAddressSource = ipAddressSource;
		this.topic = topic;
		this.flag = flag;
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		// @formatter:off
		TypeInformation<?>[] types = new TypeInformation[] {
				Types.INT, 
				Types.STRING, 
				Types.INT, 
				Types.STRING,
				Types.INT, 
				Types.LONG, 
				Types.DOUBLE, 
				Types.STRING
		};
		String[] names = new String[] {
				this.flag + SENSOR_ID, 
				this.flag + SENSOR_TYPE, 
				this.flag + PLATFORM_ID, 
				this.flag + PLATFORM_TYPE, 
				this.flag + STATION_ID,
				this.flag + TIMESTAMP, 
				this.flag + VALUE, 
				this.flag + TRIP
		};
		// @formatter:on
		return new RowTypeInfo(types, names);
	}

	@Override
	public TableSchema getTableSchema() {
		// @formatter:off
		TypeInformation<?>[] types = new TypeInformation[] {
				Types.INT, 
				Types.STRING, 
				Types.INT, 
				Types.STRING,
				Types.INT, 
				Types.LONG, 
				Types.DOUBLE, 
				Types.STRING, 
				Types.SQL_TIMESTAMP
		};
		String[] names = new String[] {
				this.flag + SENSOR_ID, 
				this.flag + SENSOR_TYPE, 
				this.flag + PLATFORM_ID, 
				this.flag + PLATFORM_TYPE, 
				this.flag + STATION_ID,
				this.flag + TIMESTAMP, 
				this.flag + VALUE, 
				this.flag + TRIP, 
				this.flag + EVENTTIME
		};
		// @formatter:on
		return new TableSchema(names, types);
	}

	@Override
	public String explainSource() {
		return "SensorTuples";
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		// @formatter:off
		return execEnv.addSource(new MqttSensorTupleConsumer(ipAddressSource, topic))
				.assignTimestampsAndWatermarks(new MyTimestampExtractor(Time.seconds(2)))
				.map(new SensorTupleToRow())
				.returns(getReturnType());
		// @formatter:on
	}

	@Override
	public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
		RowtimeAttributeDescriptor descriptor = new RowtimeAttributeDescriptor(this.flag + EVENTTIME,
				new StreamRecordTimestamp(), new PreserveWatermarks());
		return Collections.singletonList(descriptor);
	}

	public static class SensorTupleToRow
			implements MapFunction<Tuple8<Integer, String, Integer, String, Integer, Long, Double, String>, Row> {
		private static final long serialVersionUID = 8708168765594504130L;

		@Override
		public Row map(Tuple8<Integer, String, Integer, String, Integer, Long, Double, String> value) throws Exception {
			return Row.of(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, value.f6, value.f7);
		}
	}

	public static class MyTimestampExtractor extends
			BoundedOutOfOrdernessTimestampExtractor<Tuple8<Integer, String, Integer, String, Integer, Long, Double, String>> {
		private static final long serialVersionUID = 5078817907803385949L;

		public MyTimestampExtractor(Time maxOutOfOrderness) {
			super(maxOutOfOrderness);
		}

		@Override
		public long extractTimestamp(Tuple8<Integer, String, Integer, String, Integer, Long, Double, String> element) {
			return element.f5.longValue();
		}
	}
}

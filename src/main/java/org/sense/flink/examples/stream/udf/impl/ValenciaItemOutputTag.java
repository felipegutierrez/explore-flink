package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.OutputTag;
import org.sense.flink.util.ValenciaItemType;

public class ValenciaItemOutputTag extends OutputTag<Tuple2<ValenciaItemType, Long>> {
	private static final long serialVersionUID = 1024854737660323566L;

	public ValenciaItemOutputTag(String id) {
		super(id);
	}
}

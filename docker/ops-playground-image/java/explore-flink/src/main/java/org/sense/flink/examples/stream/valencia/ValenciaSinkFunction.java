package org.sense.flink.examples.stream.valencia;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ValenciaSinkFunction implements SinkFunction<String> {
    public static final List<String> values = Collections.synchronizedList(new ArrayList<String>());

    public void invoke(String value, Context context) throws Exception {
        System.out.println(value);
        values.add(value);
    }
}

package org.sense.flink.examples.stream.valencia;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;
import org.sense.flink.source.ValenciaItemConsumer;
import org.sense.flink.util.ValenciaItemType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class ValenciaDataSkewedJoinExampleTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster;
    private final int minAvailableProcessors = 4;
    private final boolean runInParallel;

    public ValenciaDataSkewedJoinExampleTest() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        this.runInParallel = availableProcessors >= minAvailableProcessors;
        if (this.runInParallel) {
            flinkCluster = new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(minAvailableProcessors)
                            .setNumberTaskManagers(1)
                            .build());
        }
    }

    @Test
    public void integrationTest() throws Exception {

        ValenciaItemConsumer sourceTraffic = new ValenciaItemConsumer(ValenciaItemType.TRAFFIC_JAM, Time.seconds(2).toMilliseconds(),
                true, true, false, 30L, false, 10);
        ValenciaItemConsumer sourcePollution = new ValenciaItemConsumer(ValenciaItemType.AIR_POLLUTION, Time.seconds(2).toMilliseconds(),
                true, true, false, 30L, false, 10);
        MySinkFunction.values.clear();

        ValenciaDataSkewedJoinExample joinExample = new ValenciaDataSkewedJoinExample("127.0.0.1", "127.0.0.1",
                sourceTraffic, sourcePollution, new MySinkFunction());
        joinExample.execute();

        List<String> results = MySinkFunction.values;
        assertEquals(16, results.size());
    }

    private static class MySinkFunction implements SinkFunction<String> {
        public static final List<String> values = Collections.synchronizedList(new ArrayList<String>());

        public void invoke(String value, Context context) throws Exception {
            System.out.println(value);
            values.add(value);
        }
    }
}

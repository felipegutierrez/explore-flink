package org.sense.flink.examples.stream.edgent;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class TemperatureAverageExampleIntegrationTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testCompletePipeline() throws Exception {
        TemperatureAverageExample.TemperatureSourceFunction source = new TemperatureAverageExample.
                TemperatureSourceFunction(Arrays.asList(
                new Tuple2<>(0, 10.0), new Tuple2<>(1, 22.0), new Tuple2<>(2, 28.0),
                new Tuple2<>(0, 10.0), new Tuple2<>(1, 22.0), new Tuple2<>(2, 28.0)), 500);
        TemperatureAverageExample.TemperatureSinkFunction sink = new TemperatureAverageExample.TemperatureSinkFunction();
        TemperatureAverageExample job = new TemperatureAverageExample(source, sink);

        job.execute();

        Map<Integer, Double> result = TemperatureAverageExample.TemperatureSinkFunction.result;
        assertEquals(3, result.size());

        Map<Integer, Double> expected = new HashMap<Integer, Double>();
        expected.put(0, 10.0);
        expected.put(1, 22.0);
        expected.put(2, 28.0);

        assertTrue(expected.equals(result));
    }
}
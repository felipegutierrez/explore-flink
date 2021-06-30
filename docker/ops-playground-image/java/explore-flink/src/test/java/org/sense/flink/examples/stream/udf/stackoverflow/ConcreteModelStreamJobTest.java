package org.sense.flink.examples.stream.udf.stackoverflow;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConcreteModelStreamJobTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster;
    private final int minAvailableProcessors = 4;
    private final boolean runInParallel;

    public ConcreteModelStreamJobTest() {
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
    public void execute() throws Exception {
        List<AbstractDataModel> abstractDataModelList = new ArrayList<AbstractDataModel>();
        abstractDataModelList.add(new ConcreteModel("a", "1"));
        abstractDataModelList.add(new ConcreteModel("a", "2"));
        MySinkFunction.values.clear();

        ConcreteModelStreamJob streamJob = new ConcreteModelStreamJob(abstractDataModelList, new MySinkFunction());
        streamJob.execute();

        List<String> results = MySinkFunction.values;
        assertEquals(2, results.size());
        assertTrue(results.containsAll(Arrays.asList("1", "2")));
    }

    private static class MySinkFunction implements SinkFunction<String> {
        public static final List<String> values = Collections.synchronizedList(new ArrayList<String>());

        public void invoke(String value, Context context) throws Exception {
            System.out.println(value);
            values.add(value);
        }
    }
}
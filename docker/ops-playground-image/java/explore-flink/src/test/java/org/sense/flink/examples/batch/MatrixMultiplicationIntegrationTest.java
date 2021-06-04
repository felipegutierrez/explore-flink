package org.sense.flink.examples.batch;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class MatrixMultiplicationIntegrationTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void testMatrixMultiplicationPipeline() throws Exception {
        String sink = "out/matrix/result.csv";
        MatrixMultiplication matrixMultiplicationJob = new MatrixMultiplication(
                "src/main/resources/matrixA.csv", "src/main/resources/matrixB.csv", sink
        );

        matrixMultiplicationJob.execute();

        File sinkFile = new File(sink);
        assertTrue(sinkFile.exists());

        try (BufferedReader br = new BufferedReader(new FileReader(sinkFile))) {
            String line;
            List<String> linesRead = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                System.out.println(line);
                linesRead.add(line);
            }
            assertEquals(4, linesRead.size());
            assertTrue(linesRead.contains("(2,1),23"));
            assertTrue(linesRead.contains("(1,1),1"));
            assertTrue(linesRead.contains("(1,2),-9"));
            assertTrue(linesRead.contains("(2,2),4"));
        }

        sinkFile.delete();
        assertFalse(sinkFile.exists());
    }
}
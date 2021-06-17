package org.sense.flink.examples.stream.edgent;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.sense.flink.examples.stream.edgent.ExceptionSimulatorProcess.POISON_TRANSACTION_ID;

public class ExceptionSimulatorProcessTest {

    @Test
    public void testMapDummyTransactionId() throws Exception {
        long referenceCurrentMillis = 900_000_000_000L;
        long referenceMillisAhead = 0L;

        // instantiate user-defined function
        ExceptionSimulatorProcess processFunction = new ExceptionSimulatorProcess(referenceCurrentMillis, referenceMillisAhead);

        // wrap user defined function into a the corresponding operator
        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> harness =
                ProcessFunctionTestHarnesses.forProcessFunction(processFunction);

        //push (timestamped) elements into the operator (and hence user defined function)
        Tuple2<String, Integer> value01 = Tuple2.of("oi", 1);
        harness.processElement(value01, 10);

        //retrieve list of emitted records for assertions
        assertEquals(harness.extractOutputValues(), Collections.singletonList(value01));
    }

    @Test
    public void testMapRandomTransactionId() throws Exception {

        long referenceCurrentMillis = 900_000_000_000L;
        long referenceMillisAhead = 2000L;

        // instantiate user-defined function
        ExceptionSimulatorProcess processFunction = new ExceptionSimulatorProcess(referenceCurrentMillis, referenceMillisAhead);

        // wrap user defined function into a the corresponding operator
        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> harness =
                ProcessFunctionTestHarnesses.forProcessFunction(processFunction);

        //push (timestamped) elements into the operator (and hence user defined function)
        Tuple2<String, Integer> value01 = Tuple2.of(UUID.randomUUID().toString(), 1);
        harness.processElement(value01, referenceCurrentMillis + 10);

        //retrieve list of emitted records for assertions
        assertEquals(harness.extractOutputValues(), Collections.singletonList(value01));
    }

    @Test(expected = SimulatedException.class)
    public void testMapException() throws Exception {
        long referenceCurrentMillis = 900_000_000_000L;
        long referenceMillisAhead = 2000L;

        // instantiate user-defined function
        ExceptionSimulatorProcess processFunction = new ExceptionSimulatorProcess(referenceCurrentMillis, referenceMillisAhead);

        // wrap user defined function into a the corresponding operator
        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> harness =
                ProcessFunctionTestHarnesses.forProcessFunction(processFunction);

        //push (timestamped) elements into the operator (and hence user defined function)
        Tuple2<String, Integer> value01 = Tuple2.of(POISON_TRANSACTION_ID, 1);
        harness.processElement(value01, referenceCurrentMillis + 10);

        //retrieve list of emitted records for assertions
        assertEquals(harness.extractOutputValues(), Collections.singletonList(value01));
    }
}
package org.sense.flink.examples.stream.udf.impl;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.Test;
import org.sense.flink.mqtt.MqttSensor;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;

public class CompositeKeySensorTypePlatformStationKeyedProcessFunctionTest {

    @Test
    public void processElement() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<Byte, MqttSensor, String> harness = setupHarness();

        Tuple5<Integer, String, Integer, String, Integer> key = new Tuple5<Integer, String, Integer, String, Integer>(0, "T", 2, "T2", 1);
        MqttSensor mqttSensorValue = new MqttSensor("topic", key, 0L, 22.2, "trip");

        harness.processElement(new StreamRecord<>(mqttSensorValue));

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>(new Tuple2<>(mqttSensorValue, 0)));

        // retrieve list of emitted records for assertions
        // assertEquals(harness.extractOutputValues().size(), Arrays.asList(Collections.singletonList(mqttSensorValue)).get(0).size());
        ConcurrentLinkedQueue<Object> actualOutput = harness.getOutput();
        System.out.println(actualOutput.size());
        // TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, actualOutput);
    }

    private KeyedOneInputStreamOperatorTestHarness<Byte, MqttSensor, String> setupHarness() throws Exception {
        // instantiate operator
        KeyedProcessOperator<Byte, MqttSensor, String> keyedProcessOperator =
                new KeyedProcessOperator<>(new CompositeKeySensorTypePlatformStationKeyedProcessFunction(0));

        // setup test harness
        Tuple5<Integer, String, Integer, String, Integer> key = new Tuple5<Integer, String, Integer, String, Integer>(1, "COUNT_PE", 2, "CIT", 1);
        MqttSensor mqttSensorMessage = new MqttSensor("mqtt_topic", key, 0L, 0.0, "trip");
        KeyedOneInputStreamOperatorTestHarness<Byte, MqttSensor, String> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<Byte, MqttSensor, String>(
                        keyedProcessOperator,
                        (MqttSensor mqttSensor) -> mqttSensor.getValue().byteValue(), // (MqttSensor mqttSensor) -> new NullByteKeySelector<>(),
                        BasicTypeInfo.BYTE_TYPE_INFO
                );

        testHarness.setup();
        testHarness.open();

        return testHarness;
    }
}
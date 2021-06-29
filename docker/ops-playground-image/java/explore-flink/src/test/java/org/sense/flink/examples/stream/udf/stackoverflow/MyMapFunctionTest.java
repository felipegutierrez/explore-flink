package org.sense.flink.examples.stream.udf.stackoverflow;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MyMapFunctionTest {

    @Test
    public void map() throws Exception {
        // instantiate your function
        MyMapFunction mapFunction = new MyMapFunction();

        AbstractDataModel concreteModel01 = new ConcreteModel("a", "1");
        AbstractDataModel concreteModel02 = new ConcreteModel("a", "2");

        // call the methods that you have implemented
        assertEquals("1", mapFunction.map(concreteModel01));
        assertEquals("2", mapFunction.map(concreteModel02));
    }
}
package org.sense.flink.examples.stream.tpch.udf;

import org.junit.Test;
import org.sense.flink.examples.stream.tpch.pojo.Customer;

import static org.junit.Assert.assertEquals;

public class CustomerFilterTest {

    @Test
    public void filterFalse() {
        // instantiate your function
        CustomerFilter filterFunction = new CustomerFilter("AUTOMOBILE");

        Customer order = new Customer(1L, 2L, "name", "address", 3L, "phone",
                4L, "marketSegment", "comment");

        assertEquals(false, filterFunction.filter(order));
    }

    @Test
    public void filterTrue() {
        // instantiate your function
        CustomerFilter filterFunction = new CustomerFilter("AUTOMOBILE");

        Customer order = new Customer(1L, 2L, "name", "address", 3L, "phone",
                4L, "AUTOMOBILE", "comment");

        assertEquals(true, filterFunction.filter(order));
    }
}
package org.sense.flink.examples.batch;

import com.sun.tools.javac.util.List;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;
import org.sense.flink.util.CustomFlinkException;

import static org.junit.Assert.assertEquals;

public class MatrixMultiplicationTest {

    @Test
    public void testSumMapper() throws Exception {
        MatrixMultiplication.SumMapper sumMapper = new MatrixMultiplication.SumMapper();

        Tuple2<Tuple3<Integer, Integer, Integer>, Integer> input = Tuple2.of(Tuple3.of(1, 2, 3), 4);
        Tuple2<Tuple2<Integer, Integer>, Integer> expected = Tuple2.of(Tuple2.of(1, 2), 4);
        assertEquals(expected, sumMapper.map(input));
    }

    @Test
    public void testCountMapperMatrixA() throws Exception {
        MatrixMultiplication.CountMapper countMapper = new MatrixMultiplication.CountMapper();

        Tuple4<String, Integer, Integer, Integer> input = Tuple4.of("A", 1, 2, 3);
        Tuple2<Integer, Integer> expected = Tuple2.of(input.f1, 1);
        assertEquals(expected, countMapper.map(input));
    }

    @Test
    public void testCountMapperMatrixB() throws Exception {
        MatrixMultiplication.CountMapper countMapper = new MatrixMultiplication.CountMapper();

        Tuple4<String, Integer, Integer, Integer> input = Tuple4.of("B", 1, 2, 3);
        Tuple2<Integer, Integer> expected = Tuple2.of(input.f2, 1);
        assertEquals(expected, countMapper.map(input));
    }

    @Test(expected = CustomFlinkException.class)
    public void testCountMapperMatrixError() throws Exception {
        MatrixMultiplication.CountMapper countMapper = new MatrixMultiplication.CountMapper();

        Tuple4<String, Integer, Integer, Integer> input = Tuple4.of("C", 1, 2, 3);
        countMapper.map(input);
    }

    @Test
    public void testSumReducer() throws Exception {
        MatrixMultiplication.SumReducer sumReducer = new MatrixMultiplication.SumReducer();

        Tuple2<Tuple2<Integer, Integer>, Integer> input1 = Tuple2.of(Tuple2.of(1, 2), 4);
        Tuple2<Tuple2<Integer, Integer>, Integer> input2 = Tuple2.of(Tuple2.of(1, 2), 4);
        Tuple2<Tuple2<Integer, Integer>, Integer> expected = Tuple2.of(Tuple2.of(input1.f0.f0, input1.f0.f1), input1.f1 + input2.f1);
        assertEquals(expected, sumReducer.reduce(input1, input2));
    }

    @Test
    public void testCountReducer() throws Exception {
        MatrixMultiplication.CountReducer countReducer = new MatrixMultiplication.CountReducer();

        Tuple2<Integer, Integer> input1 = Tuple2.of(1, 2);
        Tuple2<Integer, Integer> input2 = Tuple2.of(1, 3);
        Tuple2<Integer, Integer> expected = Tuple2.of(input1.f0, input1.f1 + input2.f1);
        assertEquals(expected, countReducer.reduce(input1, input2));
    }

    @Test
    public void testProductReducer() throws Exception {
        MatrixMultiplication.ProductReducer productReducer = new MatrixMultiplication.ProductReducer();

        Tuple2<Tuple3<Integer, Integer, Integer>, Integer> input1 = Tuple2.of(Tuple3.of(1, 2, 3), 4);
        Tuple2<Tuple3<Integer, Integer, Integer>, Integer> input2 = Tuple2.of(Tuple3.of(1, 2, 3), 4);
        Tuple2<Tuple3<Integer, Integer, Integer>, Integer> expected = Tuple2.of(input1.f0, input1.f1 * input2.f1);
        assertEquals(expected, productReducer.reduce(input1, input2));
    }

    @Test
    public void testMapMatrixToKeysAndValues() throws Exception {
        MatrixMultiplication.MapMatrixToKeysAndValues mapMatrixToKeysAndValues = new MatrixMultiplication.MapMatrixToKeysAndValues(1);

        Collector<Tuple2<Tuple3<Integer, Integer, Integer>, Integer>> collector = Mockito.mock(Collector.class);
        Iterable<Tuple4<String, Integer, Integer, Integer>> values = List.of(
                Tuple4.of("A", 1, 2, 3), Tuple4.of("B", 4, 5, 6), Tuple4.of("C", 7, 8, 9)
        );
        Tuple2<Tuple3<Integer, Integer, Integer>, Integer> expected = Tuple2.of(Tuple3.of(1, 1, 3), 3);

        mapMatrixToKeysAndValues.mapPartition(values, collector);

        Mockito.verify(collector, Mockito.times(1)).collect(expected);
    }
}
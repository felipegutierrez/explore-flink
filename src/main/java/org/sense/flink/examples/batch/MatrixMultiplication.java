package org.sense.flink.examples.batch;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class MatrixMultiplication {

	/**
	 * <code>
	 * matrix A
	 * | 1  3  4 -2|
	 * | 6  2 -3  1|
	 * file matrixA.csv
	 * 1,1,1
	 * 1,2,3
	 * 1,3,4
	 * 1,4,-2
	 * 2,1,6
	 * 2,2,2
	 * 2,3,-3
	 * 2,4,1
	 * 
	 * matrix B
	 * | 1 -2|
	 * | 4  3|
	 * |-3 -2|
	 * | 0  4|
	 * file matrixB.csv
	 * 1,1,1
	 * 1,2,-2
	 * 2,1,4
	 * 2,2,3
	 * 3,1,-3
	 * 3,2,-2
	 * 4,1,0
	 * 4,2,4
	 * </code>
	 */
	public MatrixMultiplication() {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<Integer, Integer, Integer>> matrixA = env.readCsvFile("resources/matrixA.csv")
				.fieldDelimiter(",").types(Integer.class, Integer.class, Integer.class);

		DataSet<Tuple3<Integer, Integer, Integer>> matrixB = env.readCsvFile("resources/matrixB.csv")
				.fieldDelimiter(",").types(Integer.class, Integer.class, Integer.class);

		// DataSet<Tuple2<Tuple3<Integer, Integer, Integer>, Integer>> keyValueMatrixA =
		// matrixA.map(new MapMatrixToKeysAndValues(2));

		// keyValueMatrixA.coGroup(keyValueMatrixB).where(0).equals(0).with(new
		// MyCoGroup());
	}

	/**
	 * <code>
	 * matrix A:
	 * for each element (i,j) of matrix A, emit key(i,k,i+j) and value(A[i,j]) for k in 1 until N.
	 * where:
	 * N is the number of columns on matrix B.
	 * 
	 * matrix B:
	 * for each element (j,k) of matrix B, emit key(i,k,j+k) and value(B[j,k]) for i in 1 until L.
	 * where:
	 * L is the number of lines on matrix A.
	 * </code>
	 * 
	 * This method receives matrix A and matrix B in a Tuple2<> and return a
	 * key/value in a tuple Tuple2<>
	 * 
	 * <code>
	 * matrix A = Tuple3<Integer, Integer, Integer>
	 * matrix B = Tuple3<Integer, Integer, Integer>
	 * key returned: Tuple3<Integer, Integer, Integer>
	 * value returned: Integer
	 * </code>
	 * 
	 * @author Felipe Oliveira Gutierrez
	 *
	 */
	public static class MapMatrixToKeysAndValues implements
			MapPartitionFunction<Tuple3<Integer, Integer, Integer>, Tuple2<Tuple3<Integer, Integer, Integer>, Integer>> {

		int numberOfIterations;

		public MapMatrixToKeysAndValues(int numberOfIterations) {
			this.numberOfIterations = numberOfIterations;
		}

		@Override
		public void mapPartition(Iterable<Tuple3<Integer, Integer, Integer>> matrix,
				Collector<Tuple2<Tuple3<Integer, Integer, Integer>, Integer>> out) throws Exception {

			for (int k = 1; k <= numberOfIterations; k++) {
				for (Tuple3<Integer, Integer, Integer> tuple : matrix) {
					// key(i,k, i+j) for k=1...N
					Tuple3 key = new Tuple3<Integer, Integer, Integer>(tuple.f0, k, tuple.f0 + tuple.f1);
					// value matrix[i,j]
					Integer value = 0;

					out.collect(new Tuple2<Tuple3<Integer, Integer, Integer>, Integer>(key, value));
				}
			}
		}
	}

	public static class MyCoGroup implements CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Double>, Double> {
		@Override
		public void coGroup(Iterable<Tuple2<String, Integer>> iVals, Iterable<Tuple2<String, Double>> dVals,
				Collector<Double> out) {

			Set<Integer> ints = new HashSet<Integer>();

			// add all Integer values in group to set
			for (Tuple2<String, Integer> val : iVals) {
				ints.add(val.f1);
			}

			// multiply each Double value with each unique Integer values of group
			for (Tuple2<String, Double> val : dVals) {
				for (Integer i : ints) {
					out.collect(val.f1 * i);
				}
			}
		}
	}

	/**
	 * 
	 * <code>
	 * This is the dot product of the result in 4 reducers because the matrix AB =
	 * | 1 -9|
	 * |23  4|
	 * 
	 * So, the reduces on AB_1,1 will need all the values of A_1,* and B_*,1. 8 values in total.
	 * Reduce function: emit key = (i,k) and value = SUM_j(A[i,j] * B[j,k])
	 * 
	 * </code>
	 * 
	 * @author Felipe Oliveira Gutierrez
	 */
	public static class ProductReducer implements ReduceFunction<Tuple3<Integer, Integer, Integer>> {
		@Override
		public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> v1,
				Tuple3<Integer, Integer, Integer> v2) {
			Integer sum = v1.f1 + v2.f1;
			return new Tuple3<Integer, Integer, Integer>(v1.f0, v2.f0, sum);
		}
	}

}

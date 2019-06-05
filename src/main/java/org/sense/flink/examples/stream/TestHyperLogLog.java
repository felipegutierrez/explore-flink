package org.sense.flink.examples.stream;

import java.util.Random;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

public class TestHyperLogLog {

	private static Random prng = new Random();

	public void testComputeCount() {
		HyperLogLog hyperLogLog = new HyperLogLog(16);
		hyperLogLog.offer(0);
		hyperLogLog.offer(1);
		hyperLogLog.offer(2);
		hyperLogLog.offer(3);
		hyperLogLog.offer(16);
		hyperLogLog.offer(17);
		hyperLogLog.offer(18);
		hyperLogLog.offer(19);
		hyperLogLog.offer(19);
		System.out.println("testComputeCount: " + hyperLogLog.cardinality());
		System.out.println("-------------------------");
	}

	public void testHighCardinality() {
		System.out.println("testHighCardinality");
		long start = System.currentTimeMillis();
		HyperLogLog hyperLogLog = new HyperLogLog(10);
		int size = 10000000;
		for (int i = 0; i < size; i++) {
			hyperLogLog.offer(streamElement(i));
		}
		System.out.println("time: " + (System.currentTimeMillis() - start));
		long estimate = hyperLogLog.cardinality();
		double err = Math.abs(estimate - size) / (double) size;

		System.out.println("estimate: " + estimate);
		System.out.println("size:     " + size);
		System.out.println("Error:    " + err);
		System.out.println("-------------------------");
	}

	public void testHighCardinality_withDefinedRSD() {
		System.out.println("testHighCardinality_withDefinedRSD");
		long start = System.currentTimeMillis();
		HyperLogLog hyperLogLog = new HyperLogLog(0.01);
		int size = 100000000;
		for (int i = 0; i < size; i++) {
			hyperLogLog.offer(streamElement(i));
		}
		System.out.println("time: " + (System.currentTimeMillis() - start));
		long estimate = hyperLogLog.cardinality();
		double err = Math.abs(estimate - size) / (double) size;
		System.out.println("estimate: " + estimate);
		System.out.println("size:     " + size);
		System.out.println("Error:    " + err);
		System.out.println("-------------------------");
	}

	public static Object streamElement(int i) {
		return Long.toHexString(prng.nextLong());
	}

	public static void main(String[] args) throws Exception {
		TestHyperLogLog testHyperLogLog = new TestHyperLogLog();
		testHyperLogLog.testComputeCount();
		testHyperLogLog.testHighCardinality();
		testHyperLogLog.testHighCardinality_withDefinedRSD();
	}
}

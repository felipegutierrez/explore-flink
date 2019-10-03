package org.sense.flink.util;

import com.clearspring.analytics.stream.quantile.TDigest;

public class QuantileTests {

	public static void main(String[] args) {
		System.out.println("TDigest >>> ");
		// @formatter:off
		TDigest tDigest = new TDigest(10.0);
		/*
		
		for (int i = 0; i < 10; i++) { tDigest.add(20); }
		for (int i = 0; i < 10; i++) { tDigest.add(30); }
		for (int i = 0; i < 10; i++) { tDigest.add(40); }
		for (int i = 0; i < 10; i++) { tDigest.add(50); }
		for (int i = 0; i < 10; i++) { tDigest.add(60); }
		for (int i = 0; i < 10; i++) { tDigest.add(70); }
		for (int i = 0; i < 10; i++) { tDigest.add(80); }
		for (int i = 0; i < 10; i++) { tDigest.add(90); }
		for (int i = 0; i < 10; i++) { tDigest.add(100); }
		 */
		for (int i = 0; i < 100; i++) { tDigest.add(10, 1); }
		tDigest.add(20, 1);
		tDigest.add(30, 1);
		tDigest.add(40, 1);
		tDigest.add(50, 1);
		tDigest.add(60, 1);
		tDigest.add(70, 1);
		tDigest.add(80, 1);
		tDigest.add(90, 1);
		tDigest.add(100, 1);
		System.out.println("the approximate fraction of all samples that were less than or equal to x.");
		for (int i = 1; i <= 10; i++) { System.out.println("tDigest.cdf(" + (i * 10) + "): " + tDigest.cdf((i * 10))); }
		// @formatter:on
		System.out.println("tDigest.quantile(0.1): " + tDigest.quantile(0.1));

		/*
		System.out.println();
		System.out.println("QDigest >>> ");
		// @formatter:off
		QDigest qDigest = new QDigest(1000.0);
		for (int i = 0; i < 100; i++) { qDigest.offer(10); }
		for (int i = 0; i < 10; i++) { qDigest.offer(20); }
		for (int i = 0; i < 10; i++) { qDigest.offer(30); }
		for (int i = 0; i < 10; i++) { qDigest.offer(40); }
		for (int i = 0; i < 10; i++) { qDigest.offer(50); }
		for (int i = 0; i < 10; i++) { qDigest.offer(60); }
		for (int i = 0; i < 10; i++) { qDigest.offer(70); }
		for (int i = 0; i < 10; i++) { qDigest.offer(80); }
		for (int i = 0; i < 10; i++) { qDigest.offer(90); }
		for (int i = 0; i < 10; i++) { qDigest.offer(100); }
		for (int i = 1; i <= 10; i++) { System.out.println("qDigest.getQuantile(" + (i * 10) + "): " + qDigest.getQuantile((i * 10))); }
		// @formatter:on
		System.out.println("qDigest.computeActualSize(): " + qDigest.computeActualSize());
		List<long[]> list = qDigest.toAscRanges();
		for (long[] arr : list) {
			for (int i = 0; i < arr.length; i++) {
				System.out.println("arr[" + i + "]: " + arr[i]);
			}
		}
		*/
	}
}

package org.sense.flink.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CountMinSketch {

	private static final int H1 = 0;
	private static final int H2 = 1;
	private static final int H3 = 2;
	private static final int H4 = 3;
	private static final int LIMIT = 100;
	private final int[][] sketch = new int[4][LIMIT];

	final NaiveHashFunction h1 = new NaiveHashFunction(11, 9);
	final NaiveHashFunction h2 = new NaiveHashFunction(17, 15);
	final NaiveHashFunction h3 = new NaiveHashFunction(31, 65);
	final NaiveHashFunction h4 = new NaiveHashFunction(61, 101);

	private ExecutorService executor = Executors.newSingleThreadExecutor();

	public CountMinSketch() {
		// initialize sketch
	}

	public Future<Boolean> updateSketch(String value) {
		return executor.submit(() -> {
			sketch[H1][h1.getHashValue(value)]++;
			sketch[H2][h2.getHashValue(value)]++;
			sketch[H3][h3.getHashValue(value)]++;
			sketch[H4][h4.getHashValue(value)]++;
			return true;
		});
	}

	public int getFrequencyFromSketch(String value) {
		int valueH1 = sketch[H1][h1.getHashValue(value)];
		int valueH2 = sketch[H2][h2.getHashValue(value)];
		int valueH3 = sketch[H3][h3.getHashValue(value)];
		int valueH4 = sketch[H4][h4.getHashValue(value)];
		return findMinimum(valueH1, valueH2, valueH3, valueH4);
	}

	private int findMinimum(final int a, final int b, final int c, final int d) {
		return Math.min(Math.min(a, b), Math.min(c, d));
	}
}

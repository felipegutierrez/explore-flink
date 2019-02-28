package org.sense.flink.util;

import java.io.Serializable;

public class CountMinSketch implements Serializable {

	private static final long serialVersionUID = 1123747953291780413L;

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

	public CountMinSketch() {
		// initialize sketch
	}

	public Boolean updateSketch(String value) {
		sketch[H1][h1.getHashValue(value)]++;
		sketch[H2][h2.getHashValue(value)]++;
		sketch[H3][h3.getHashValue(value)]++;
		sketch[H4][h4.getHashValue(value)]++;

		return true;
	}

	public Boolean updateSketchAsync(String value) {
		new Thread(() -> {
			sketch[H1][h1.getHashValue(value)]++;
		}).start();
		new Thread(() -> {
			sketch[H2][h2.getHashValue(value)]++;
		}).start();
		new Thread(() -> {
			sketch[H3][h3.getHashValue(value)]++;
		}).start();
		new Thread(() -> {
			sketch[H4][h4.getHashValue(value)]++;
		}).start();
		return true;
	}

	public Boolean updateSketch(String value, int count) {
		sketch[H1][h1.getHashValue(value)] = sketch[H1][h1.getHashValue(value)] + count;
		sketch[H2][h2.getHashValue(value)] = sketch[H2][h2.getHashValue(value)] + count;
		sketch[H3][h3.getHashValue(value)] = sketch[H3][h3.getHashValue(value)] + count;
		sketch[H4][h4.getHashValue(value)] = sketch[H4][h4.getHashValue(value)] + count;
		return true;
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

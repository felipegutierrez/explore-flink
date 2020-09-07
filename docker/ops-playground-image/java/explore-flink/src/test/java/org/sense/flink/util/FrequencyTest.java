package org.sense.flink.util;

import com.clearspring.analytics.stream.frequency.IFrequency;

public class FrequencyTest {
	public static void main(String[] args) {
		IFrequency frequency = new com.clearspring.analytics.stream.frequency.CountMinSketch(10, 5, 0);
		// @formatter:off
		for (int i = 0; i < 10; i++) { frequency.add(1, 1); }
		for (int i = 0; i < 10; i++) { frequency.add(2, 1); }
		for (int i = 0; i < 10; i++) { frequency.add(3, 1); }
		for (int i = 0; i < 10; i++) { frequency.add(4, 1); }
		for (int i = 0; i < 10; i++) { frequency.add(5, 1); }
		for (int i = 0; i < 10; i++) { frequency.add(6, 1); }
		for (int i = 0; i < 10; i++) { frequency.add(7, 1); }
		for (int i = 0; i < 10; i++) { frequency.add(8, 1); }
		for (int i = 0; i < 10; i++) { frequency.add(9, 1); }
		for (int i = 0; i < 10; i++) { frequency.add(10, 1); }
		for (int i = 1; i <= 10; i++) { System.out.println("frequency.estimateCount(" + i + "): " + frequency.estimateCount(i)); }
		// @formatter:on
	}
}

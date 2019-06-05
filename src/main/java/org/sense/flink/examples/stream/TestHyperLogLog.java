package org.sense.flink.examples.stream;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;

public class TestHyperLogLog {

	private static Random prng = new Random();

	public void testHllEstimation() throws FileNotFoundException, IOException {
		// download https://www.gutenberg.org/files/100/100-0.txt at
		// explore-flink/out/100-0.txt
		System.out.println("testHllEstimation");
		String file = "out/100-0.txt";

		HyperLogLog hyperLogLog = new HyperLogLog(16);

		List<String> list = new ArrayList<String>();
		String[] tempArray;
		String delimiter = " ";

		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = br.readLine()) != null) {
				tempArray = line.split(delimiter);
				for (int i = 0; i < tempArray.length; i++) {
					hyperLogLog.offer(tempArray[i]);
					if (!list.contains(tempArray[i])) {
						list.add(tempArray[i]);
					}
				}
			}
		}

		long size = list.size();
		long estimate = hyperLogLog.cardinality();
		double err = Math.abs(estimate - size) / (double) size;
		System.out.println("exact size            : " + size);
		System.out.println("cardinality estimation: " + estimate);
		System.out.println("Error                 : " + err);
		System.out.println("-------------------------");
	}

	public void testComputeCount() {
		System.out.println("testComputeCount");
		HyperLogLog hyperLogLog = new HyperLogLog(16);

		List<String> list = new ArrayList<String>();
		String[] tempArray;
		String delimiter = " ";

		String sentence = "this is a sentence about stuff";
		tempArray = sentence.split(delimiter);
		for (int i = 0; i < tempArray.length; i++) {
			hyperLogLog.offer(tempArray[i]);
			if (!list.contains(tempArray[i])) {
				list.add(tempArray[i]);
			}
		}

		sentence = "this is another sentence about stuff";
		tempArray = sentence.split(delimiter);
		for (int i = 0; i < tempArray.length; i++) {
			hyperLogLog.offer(tempArray[i]);
			if (!list.contains(tempArray[i])) {
				list.add(tempArray[i]);
			}
		}
		long size = list.size();
		long estimate = hyperLogLog.cardinality();
		double err = Math.abs(estimate - size) / (double) size;
		System.out.println("exact size            : " + size);
		System.out.println("cardinality estimation: " + estimate);
		System.out.println("Error                 : " + err);
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

		System.out.println("size:     " + size);
		System.out.println("estimate: " + estimate);
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
		System.out.println("size:     " + size);
		System.out.println("estimate: " + estimate);
		System.out.println("Error:    " + err);
		System.out.println("-------------------------");
	}

	public static Object streamElement(int i) {
		return Long.toHexString(prng.nextLong());
	}

	public static void main(String[] args) throws Exception {
		TestHyperLogLog testHyperLogLog = new TestHyperLogLog();
		testHyperLogLog.testComputeCount();
		testHyperLogLog.testHllEstimation();
		testHyperLogLog.testHighCardinality();
		testHyperLogLog.testHighCardinality_withDefinedRSD();
	}
}

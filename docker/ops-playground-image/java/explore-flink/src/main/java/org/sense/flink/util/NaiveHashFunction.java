package org.sense.flink.util;

import java.io.Serializable;

public class NaiveHashFunction implements Serializable {

	private static final long serialVersionUID = -3460094846654202562L;
	private final static int LIMIT = 100;
	private long prime;
	private long odd;

	public NaiveHashFunction(final long prime, final long odd) {
		this.prime = prime;
		this.odd = odd;
	}

	public int getHashValue(final String value) {
		int hash = value.hashCode();
		if (hash < 0) {
			hash = Math.abs(hash);
		}
		return calculateHash(hash, prime, odd);
	}

	private int calculateHash(final int hash, final long prime, final long odd) {
		return (int) ((((hash % LIMIT) * prime) % LIMIT) * odd) % LIMIT;
	}

	public static void main(String[] args) {
		final NaiveHashFunction h1 = new NaiveHashFunction(11, 9);
		final NaiveHashFunction h2 = new NaiveHashFunction(17, 15);
		final NaiveHashFunction h3 = new NaiveHashFunction(31, 65);
		final NaiveHashFunction h4 = new NaiveHashFunction(61, 101);

		String value = "A";
		System.out.println("Hash value for [" + value + "]: " + h1.getHashValue(value) + " - " + h2.getHashValue(value)
				+ " - " + h3.getHashValue(value) + " - " + h4.getHashValue(value));
		value = "AA";
		System.out.println("Hash value for [" + value + "]: " + h1.getHashValue(value) + " - " + h2.getHashValue(value)
				+ " - " + h3.getHashValue(value) + " - " + h4.getHashValue(value));
		value = "B";
		System.out.println("Hash value for [" + value + "]: " + h1.getHashValue(value) + " - " + h2.getHashValue(value)
				+ " - " + h3.getHashValue(value) + " - " + h4.getHashValue(value));
		value = "BB";
		System.out.println("Hash value for [" + value + "]: " + h1.getHashValue(value) + " - " + h2.getHashValue(value)
				+ " - " + h3.getHashValue(value) + " - " + h4.getHashValue(value));
	}
}

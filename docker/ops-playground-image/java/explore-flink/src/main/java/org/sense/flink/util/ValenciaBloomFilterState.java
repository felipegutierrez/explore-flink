package org.sense.flink.util;

import java.io.Serializable;

import com.clearspring.analytics.stream.membership.BloomFilter;
import com.clearspring.analytics.stream.membership.Filter;

public class ValenciaBloomFilterState implements Serializable {
	private static final long serialVersionUID = 5997498632763082349L;
	private Filter left;
	private Filter right;
	private long lastModified;

	public ValenciaBloomFilterState() {
		this.left = new BloomFilter(100, 0.01);
		this.right = new BloomFilter(100, 0.01);
		this.lastModified = 0L;
	}

	public long getLastModified() {
		return lastModified;
	}

	public void setLastModified(long lastModified) {
		this.lastModified = lastModified;
	}

	public Filter getLeft() {
		return left;
	}

	public void setLeft(Filter left) {
		this.left = left;
	}

	public boolean isPresentLeft(String key) {
		return left.isPresent(key);
	}

	public void addLeft(String key) {
		this.left.add(key);
	}

	public Filter getRight() {
		return right;
	}

	public void setRight(Filter right) {
		this.right = right;
	}

	public boolean isPresentRight(String key) {
		return right.isPresent(key);
	}

	public void addRight(String key) {
		this.right.add(key);
	}
}
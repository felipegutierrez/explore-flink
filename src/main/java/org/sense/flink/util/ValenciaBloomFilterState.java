package org.sense.flink.util;

import java.io.Serializable;

import com.clearspring.analytics.stream.membership.BloomFilter;
import com.clearspring.analytics.stream.membership.Filter;

public class ValenciaBloomFilterState implements Serializable {
	private static final long serialVersionUID = 5997498632763082349L;
	private Filter trafficLeft;
	private Filter trafficRight;
	private Filter pollutionLeft;
	private Filter pollutionRight;
	private long lastModified;

	public ValenciaBloomFilterState() {
		this.trafficRight = new BloomFilter(100, 0.01);
		this.pollutionLeft = new BloomFilter(100, 0.01);
		this.trafficLeft = new BloomFilter(100, 0.01);
		this.pollutionRight = new BloomFilter(100, 0.01);
		this.lastModified = 0L;
	}

	public long getLastModified() {
		return lastModified;
	}

	public void setLastModified(long lastModified) {
		this.lastModified = lastModified;
	}

	public Filter getTrafficRight() {
		return trafficRight;
	}

	public void setTrafficRight(Filter traffic) {
		this.trafficRight = traffic;
	}

	public boolean isPresentTrafficRight(String key) {
		return trafficRight.isPresent(key);
	}

	public void addTrafficRight(String key) {
		this.trafficRight.add(key);
	}

	public Filter getTrafficLeft() {
		return trafficLeft;
	}

	public void setTrafficLeft(Filter traffic) {
		this.trafficLeft = traffic;
	}

	public boolean isPresentTrafficLeft(String key) {
		return trafficLeft.isPresent(key);
	}

	public void addTrafficLeft(String key) {
		this.trafficLeft.add(key);
	}

	public Filter getPollutionLeft() {
		return pollutionLeft;
	}

	public void setPollutionLeft(Filter pollution) {
		this.pollutionLeft = pollution;
	}

	public boolean isPresentPollutionLeft(String key) {
		return pollutionLeft.isPresent(key);
	}

	public void addPollutionLeft(String key) {
		this.pollutionLeft.add(key);
	}

	public Filter getPollutionRight() {
		return pollutionRight;
	}

	public void setPollutionRight(Filter pollution) {
		this.pollutionRight = pollution;
	}

	public boolean isPresentPollutionRight(String key) {
		return pollutionRight.isPresent(key);
	}

	public void addPollutionRight(String key) {
		this.pollutionRight.add(key);
	}
}
package org.sense.flink.mqtt;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.api.java.tuple.Tuple5;

public class MqttSensor implements Serializable {

	private static final long serialVersionUID = 6298037702066128180L;
	private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	private Tuple5<Integer, String, Integer, String, Integer> key;
	private String topic;
	private Long timestamp;
	private Double value;
	private String trip;

	public MqttSensor(String topic, Tuple5<Integer, String, Integer, String, Integer> key, Long timestamp, Double value,
			String trip) {
		this.topic = topic;
		this.key = key;
		this.timestamp = timestamp;
		this.value = value;
		this.trip = trip;
	}

	public Tuple5<Integer, String, Integer, String, Integer> getKey() {
		return key;
	}

	public void setKey(Tuple5<Integer, String, Integer, String, Integer> key) {
		this.key = key;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public Double getValue() {
		return value;
	}

	public void setValue(Double value) {
		this.value = value;
	}

	public String getTrip() {
		return trip;
	}

	public void setTrip(String trip) {
		this.trip = trip;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
		result = prime * result + ((trip == null) ? 0 : trip.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MqttSensor other = (MqttSensor) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (timestamp == null) {
			if (other.timestamp != null)
				return false;
		} else if (!timestamp.equals(other.timestamp))
			return false;
		if (topic == null) {
			if (other.topic != null)
				return false;
		} else if (!topic.equals(other.topic))
			return false;
		if (trip == null) {
			if (other.trip != null)
				return false;
		} else if (!trip.equals(other.trip))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "MqttSensor [key=" + key + ", topic=" + topic + ", timestamp=" + sdf.format(new Date(timestamp))
				+ ", value=" + value + ", trip=" + trip + "]";
	}
}

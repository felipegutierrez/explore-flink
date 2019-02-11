package org.sense.flink.mqtt;

import java.io.Serializable;

import org.apache.flink.api.java.tuple.Tuple4;

public class MqttSensor implements Serializable {

	private static final long serialVersionUID = 6298037702066128180L;
	private Tuple4<Integer, String, Integer, Integer> key;
	private String topic;
	private Double value;

	public MqttSensor(String topic, Tuple4<Integer, String, Integer, Integer> key, Double value) {
		this.topic = topic;
		this.key = key;
		this.value = value;
	}

	public Tuple4<Integer, String, Integer, Integer> getKey() {
		return key;
	}

	public void setKey(Tuple4<Integer, String, Integer, Integer> key) {
		this.key = key;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Double getValue() {
		return value;
	}

	public void setValue(Double value) {
		this.value = value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
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
		if (topic == null) {
			if (other.topic != null)
				return false;
		} else if (!topic.equals(other.topic))
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
		return "MqttSensor [key=" + key + ", topic=" + topic + ", value=" + value + "]";
	}
}

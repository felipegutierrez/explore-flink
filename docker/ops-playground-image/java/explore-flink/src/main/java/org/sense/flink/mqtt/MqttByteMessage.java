package org.sense.flink.mqtt;

import java.io.Serializable;
import java.util.Arrays;

public class MqttByteMessage implements Serializable {

	private static final long serialVersionUID = 6141435584784220401L;

	private String topic;

	private byte[] payload;

	public MqttByteMessage(String topic, byte[] payload) {
		this.topic = topic;
		this.payload = payload;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public byte[] getPayload() {
		return payload;
	}

	public void setPayload(byte[] payload) {
		this.payload = payload;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(payload);
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
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
		MqttByteMessage other = (MqttByteMessage) obj;
		if (!Arrays.equals(payload, other.payload))
			return false;
		if (topic == null) {
			if (other.topic != null)
				return false;
		} else if (!topic.equals(other.topic))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "MqttMessage [topic=" + topic + ", payload=" + payload + "]";
	}
}

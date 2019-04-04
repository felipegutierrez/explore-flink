package org.sense.flink.mqtt;

import java.io.Serializable;

public class CompositeKeySensorType implements Serializable {
	private static final long serialVersionUID = 158522745086544211L;

	private Integer stationId;
	private Integer platformId;
	private String sensorType;

	public CompositeKeySensorType() {
		this.stationId = null;
		this.platformId = null;
		this.sensorType = "";
	}

	public CompositeKeySensorType(Integer stationId, Integer platformId, String sensorType) {
		this.stationId = stationId;
		this.platformId = platformId;
		this.sensorType = sensorType;
	}

	public Integer getStationId() {
		return stationId;
	}

	public void setStationId(Integer stationId) {
		this.stationId = stationId;
	}

	public Integer getPlatformId() {
		return platformId;
	}

	public void setPlatformId(Integer platformId) {
		this.platformId = platformId;
	}

	public String getSensorType() {
		return sensorType;
	}

	public void setSensorType(String sensorType) {
		this.sensorType = sensorType;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((platformId == null) ? 0 : platformId.hashCode());
		result = prime * result + ((sensorType == null) ? 0 : sensorType.hashCode());
		result = prime * result + ((stationId == null) ? 0 : stationId.hashCode());
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
		CompositeKeySensorType other = (CompositeKeySensorType) obj;
		if (platformId == null) {
			if (other.platformId != null)
				return false;
		} else if (!platformId.equals(other.platformId))
			return false;
		if (sensorType == null) {
			if (other.sensorType != null)
				return false;
		} else if (!sensorType.equals(other.sensorType))
			return false;
		if (stationId == null) {
			if (other.stationId != null)
				return false;
		} else if (!stationId.equals(other.stationId))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "CompositeKeySensorType [stationId, platformId, sensorType][" + stationId + "," + platformId + ","
				+ sensorType + "]";
	}
}

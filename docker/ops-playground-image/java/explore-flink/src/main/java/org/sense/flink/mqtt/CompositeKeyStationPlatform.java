package org.sense.flink.mqtt;

import java.io.Serializable;

public class CompositeKeyStationPlatform implements Serializable {
	private static final long serialVersionUID = 468254179320463765L;

	private Integer stationId;
	private Integer platformId;

	public CompositeKeyStationPlatform() {
		this.stationId = null;
		this.platformId = null;
	}

	public CompositeKeyStationPlatform(Integer stationId, Integer platformId) {
		this.stationId = stationId;
		this.platformId = platformId;
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((platformId == null) ? 0 : platformId.hashCode());
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
		CompositeKeyStationPlatform other = (CompositeKeyStationPlatform) obj;
		if (platformId == null) {
			if (other.platformId != null)
				return false;
		} else if (!platformId.equals(other.platformId))
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
		return "CompositeKeySensorType [stationId, platformId][" + stationId + "," + platformId + "]";
	}
}

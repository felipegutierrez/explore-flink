package org.sense.flink.mqtt;

import java.io.Serializable;

/**
 * The skewParameter is filled with a round-robin number to the key that has
 * skew.
 * 
 * @author Felipe Oliveira Gutierrez
 */
public class CompositeSkewedKeyStationPlatform implements Serializable {
	private static final long serialVersionUID = -5960601544505897824L;
	private Integer stationId;
	private Integer platformId;
	private Integer skewParameter;

	public CompositeSkewedKeyStationPlatform() {
		this.stationId = null;
		this.platformId = null;
		this.skewParameter = null;
	}

	public CompositeSkewedKeyStationPlatform(Integer stationId, Integer platformId, Integer skewParameter) {
		this.stationId = stationId;
		this.platformId = platformId;
		this.skewParameter = skewParameter;
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

	public Integer getSkewParameter() {
		return skewParameter;
	}

	public void setSkewParameter(Integer skewParameter) {
		this.skewParameter = skewParameter;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((platformId == null) ? 0 : platformId.hashCode());
		result = prime * result + ((skewParameter == null) ? 0 : skewParameter.hashCode());
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
		CompositeSkewedKeyStationPlatform other = (CompositeSkewedKeyStationPlatform) obj;
		if (platformId == null) {
			if (other.platformId != null)
				return false;
		} else if (!platformId.equals(other.platformId))
			return false;
		if (skewParameter == null) {
			if (other.skewParameter != null)
				return false;
		} else if (!skewParameter.equals(other.skewParameter))
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
		return "CompositeKeySensorType [stationId, platformId, skewParameter][" + stationId + "," + platformId + ","
				+ skewParameter + "]";
	}
}

package org.sense.flink.pojo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public abstract class ValenciaItem implements Serializable {
	private static final long serialVersionUID = -107439991877659718L;
	protected Long id;
	protected Long adminLevel;
	protected String district;
	protected Date update;
	protected List<Point> coordinates;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Long getAdminLevel() {
		return adminLevel;
	}

	public void setAdminLevel(Long adminLevel) {
		this.adminLevel = adminLevel;
	}

	public Date getUpdate() {
		return update;
	}

	public void setUpdate(Date update) {
		this.update = update;
	}

	public List<Point> getCoordinates() {
		return coordinates;
	}

	public void setCoordinates(List<Point> coordinates) {
		this.coordinates = coordinates;
	}

	public void addCoordinates(Point point) {
		if (this.coordinates == null) {
			this.coordinates = new ArrayList<Point>();
		}
		this.coordinates.add(point);
	}

	public String getDistrict() {
		return district;
	}

	public void setDistrict(String district) {
		this.district = district;
	}

	@Override
	public String toString() {
		return "ValenciaItem [id=" + id + ", adminLevel=" + adminLevel + ", district=" + district + ", update=" + update
				+ ", coordinates=" + coordinates + "]";
	}

}

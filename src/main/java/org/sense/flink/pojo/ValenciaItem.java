package org.sense.flink.pojo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public abstract class ValenciaItem implements Serializable {
	private static final long serialVersionUID = -107439991877659718L;
	protected Integer id;
	protected Date update;
	protected List<Point> coordinates;

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
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

	public void addCoordinates(Point point) {
		if (this.coordinates == null) {
			this.coordinates = new ArrayList<Point>();
		}
		this.coordinates.add(point);
	}
}

package org.sense.flink.pojo;

import java.io.Serializable;
import java.util.Date;

public class ValenciaNoise extends ValenciaItem implements Serializable {
	private static final long serialVersionUID = -1833042471487904251L;
	private String street;
	private String uri;

	public ValenciaNoise() {
	}

	public ValenciaNoise(String street, String parameters, String uri, String coordinates) {
		this.id = 0;
		this.street = street;
		this.update = new Date();
		this.uri = uri;
		this.coordinates = Point.extract(coordinates);
	}

	@Override
	public String toString() {
		return "ValenciaNoise [id=" + id + ", coordinates=" + coordinates + ", street=" + street + ", update=" + update
				+ ", uri=" + uri + "]";
	}
}

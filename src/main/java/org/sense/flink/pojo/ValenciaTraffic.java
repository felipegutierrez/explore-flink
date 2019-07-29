package org.sense.flink.pojo;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class ValenciaTraffic extends ValenciaItem implements Serializable {
	private static final long serialVersionUID = -3147914413052930222L;
	private String street;
	private Integer status;
	private String uri;

	// 2019-07-22T12:51:04.681+02:00
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

	public ValenciaTraffic() {
	}

	public ValenciaTraffic(Long adminLevel, String street, String update, Integer status, String coordinates,
			String csr, String uri) {
		this.adminLevel = adminLevel;
		this.street = street;
		this.status = status;
		this.coordinates = Point.extract(coordinates, csr);
		this.uri = uri;
		try {
			this.update = formatter.parse(update);
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	public ValenciaTraffic(Long adminLevel, String street, Date update, Integer status, List<Point> coordinates) {
		this.adminLevel = adminLevel;
		this.street = street;
		this.update = update;
		this.status = status;
		this.coordinates = coordinates;
	}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public Integer getStatus() {
		return status;
	}

	public void setStatus(Integer status) {
		this.status = status;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}
}

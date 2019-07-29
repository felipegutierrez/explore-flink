package org.sense.flink.pojo;

import java.io.Serializable;
import java.util.Date;

public class ValenciaPollution extends ValenciaItem implements Serializable {
	private static final long serialVersionUID = -469085368038671321L;
	private String street;
	private AirPollution parameters;
	private String uri;

	public ValenciaPollution() {
	}

	public ValenciaPollution(String street, String parameters, String uri, String coordinates, String csr) {
		this.adminLevel = 0L;
		this.street = street;
		this.parameters = AirPollution.extract(uri);
		this.update = new Date();
		this.uri = uri;
		this.coordinates = Point.extract(coordinates, csr);
	}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public AirPollution getParameters() {
		return parameters;
	}

	public void setParameters(AirPollution parameters) {
		this.parameters = parameters;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	@Override
	public String toString() {
		return "ValenciaItem [id=" + id + ", adminLevel=" + adminLevel + ", district=" + district + ", update=" + update
				+ ", parameters=" + parameters + ", coordinates=" + coordinates + "]";
	}
}

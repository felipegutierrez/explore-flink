package org.sense.flink.pojo;

import java.util.Date;
import java.util.List;

/**
 * 
 * http://gobiernoabierto.valencia.es/en/dataset/?id=estaciones-automaticas-atmosfericas
 * 
 * @author felipe
 *
 */
public class ValenciaPollution extends ValenciaItem {
	private static final long serialVersionUID = 7482849147265587770L;
	// additional attributes
	private String street;
	private String uri;

	public ValenciaPollution(Long id, Long adminLevel, String district, Date update, List<Point> coordinates,
			Object value) {
		super(id, adminLevel, district, update, coordinates, AirPollution.extract((String) value));
	}

	public ValenciaPollution(Long id, Long adminLevel, String district, String update, String coordinates, String csr,
			Object value) {
		super(id, adminLevel, district, update, coordinates, csr, AirPollution.extract((String) value));
	}

	/** overriding default methods */
	@Override
	public Object getValue() {
		return (AirPollution) this.value;
	}

	@Override
	public void setValue(Object value) {
		this.value = (AirPollution) value;
	}

	/** specific methods */
	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}
}

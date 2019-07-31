package org.sense.flink.pojo;

import java.util.Date;
import java.util.List;

/**
 * This class was developed based on the metadata available at
 * http://gobiernoabierto.valencia.es/en/dataset/?id=estado-trafico-tiempo-real
 * 
 * @author felipe
 *
 */
public class ValenciaTraffic extends ValenciaItem {
	private static final long serialVersionUID = -3555130785188765225L;
	// additional attributes
	private String street;
	private String uri;

	public ValenciaTraffic(Long id, Long adminLevel, String district, Date update, List<Point> coordinates,
			Object value) {
		super(id, adminLevel, district, update, coordinates, value);
	}

	public ValenciaTraffic(Long id, Long adminLevel, String district, String update, String coordinates, String csr,
			Object value) {
		super(id, adminLevel, district, update, coordinates, csr, value);
	}

	/** overriding default methods */
	@Override
	public Object getValue() {
		return (Integer) this.value;
	}

	@Override
	public void setValue(Object value) {
		this.value = (Integer) value;
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

package org.sense.flink.pojo;

import java.util.Date;
import java.util.List;

import org.sense.flink.util.ValenciaItemType;

/**
 * 
 * http://gobiernoabierto.valencia.es/en/dataset/?id=estaciones-automaticas-atmosfericas
 * 
 * @author felipe
 *
 */
public class ValenciaItemAvg extends ValenciaItem {
	private static final long serialVersionUID = 1067534711709565841L;
	// additional attributes
	private String street;
	private String uri;

	public ValenciaItemAvg(Long id, Long adminLevel, String district, Date update, List<Point> coordinates,
			AirPollution value) {
		super(id, adminLevel, district, update, ValenciaItemType.AVERAGE, coordinates, value);
	}

	public ValenciaItemAvg(Long id, Long adminLevel, String district, Date update, List<Point> coordinates,
			Object value) {
		super(id, adminLevel, district, update, ValenciaItemType.AVERAGE, coordinates, value);
	}

	public ValenciaItemAvg(Long id, Long adminLevel, String district, String update, String coordinates, String csr,
			Object value) {
		super(id, adminLevel, district, update, ValenciaItemType.AVERAGE, coordinates, csr, value);
	}

	/** overriding default methods */
	@Override
	public Object getValue() {
		return (String) this.value;
	}

	@Override
	public void setValue(Object value) {
		this.value = (String) value;
	}

	@Override
	public void addValue(Object value) {
		// TODO Auto-generated method stub
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

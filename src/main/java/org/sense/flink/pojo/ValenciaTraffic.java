package org.sense.flink.pojo;

import java.text.ParseException;
import java.util.Date;
import java.util.List;

import org.sense.flink.util.ValenciaItemType;

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
		super(id, adminLevel, district, update, ValenciaItemType.TRAFFIC_JAM, coordinates, value);
		this.timestamp = update.getTime();
	}

	public ValenciaTraffic(Long id, Long adminLevel, String district, String update, String coordinates, String csr,
			Object value) throws ParseException {
		super(id, adminLevel, district, update, ValenciaItemType.TRAFFIC_JAM, coordinates, csr, value);
		this.timestamp = formatter.parse(update).getTime();
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

	@Override
	public void addValue(Object value) {
		if (this.value == null && value != null) {
			this.value = (Integer) value;
		} else {
			Integer tmp = (Integer) value;
			this.value = ((Integer) this.value + tmp) / 2;
		}
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

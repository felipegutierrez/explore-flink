package org.sense.flink.pojo;

import java.text.ParseException;
import java.util.Date;
import java.util.List;

import org.sense.flink.util.ValenciaItemType;

/**
 * 
 * @author felipe
 *
 */
public class ValenciaItemEnriched extends ValenciaItem {
	private static final long serialVersionUID = -6513446153100663934L;
	// additional attributes
	private String street;
	private String uri;
	private String distances;

	public ValenciaItemEnriched(Long id, Long adminLevel, String district, Date update, List<Point> coordinates,
			AirPollution value) {
		super(id, adminLevel, district, update, ValenciaItemType.ENRICHED, coordinates, value);
		this.timestamp = update.getTime();
	}

	public ValenciaItemEnriched(Long id, Long adminLevel, String district, Date update, List<Point> coordinates,
			Object value) {
		super(id, adminLevel, district, update, ValenciaItemType.ENRICHED, coordinates, value);
		this.timestamp = update.getTime();
	}

	public ValenciaItemEnriched(Long id, Long adminLevel, String district, String update, String coordinates,
			String csr, Object value) throws ParseException {
		super(id, adminLevel, district, update, ValenciaItemType.ENRICHED, coordinates, csr, value);
		this.timestamp = formatter.parse(update).getTime();
	}

	/** overriding default methods */
	@Override
	public Object getValue() {
		return this.value;
	}

	@Override
	public void setValue(Object value) {
		this.value = value;
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

	public String getDistances() {
		return distances;
	}

	public void setDistances(String distances) {
		this.distances = distances;
	}

	@Override
	public String toString() {
		return "ValenciaItem [type=" + type + ", id=" + id + ", adminLevel=" + adminLevel + ", district=" + district
				+ ", update=" + update + ", timestamp=" + timestamp + ", value=" + value + ", coordinates="
				+ coordinates + ", distances=" + distances + "]";
	}

}

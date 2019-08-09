package org.sense.flink.pojo;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.sense.flink.util.ValenciaItemType;

/**
 * This class is a generic class for all items from Valencia open-data web
 * portal.
 * 
 * URL: http://gobiernoabierto.valencia.es/en/data/
 * 
 * @author felipe
 *
 */
public abstract class ValenciaItem implements Cloneable, Serializable {
	private static final long serialVersionUID = 1117798428156554356L;
	// default attributes
	protected Long id;
	protected Long adminLevel;
	protected String district;
	protected Date update;
	protected List<Point> coordinates;
	protected Object value;
	protected ValenciaItemType type;
	protected Long timestamp;

	// 2019-07-22T12:51:04.681+02:00
	protected SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

	public ValenciaItem(Long id, Long adminLevel, String district, Date update, ValenciaItemType type,
			List<Point> coordinates, Object value) {
		this.id = id;
		this.adminLevel = adminLevel;
		this.district = district;
		this.update = update;
		this.type = type;
		this.coordinates = coordinates;
		this.value = value;
	}

	public ValenciaItem(Long id, Long adminLevel, String district, String update, ValenciaItemType type,
			String coordinates, String csr, Object value) throws ParseException {
		this.id = id;
		this.adminLevel = adminLevel;
		this.district = district;
		this.coordinates = Point.extract(coordinates, csr);
		this.update = formatter.parse(update);
		this.type = type;
		this.value = value;
	}

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

	public void clearCoordinates() {
		this.coordinates = new ArrayList<Point>();
	}

	public void addCoordinates(Point point) {
		if (this.coordinates == null) {
			this.coordinates = new ArrayList<Point>();
		}
		this.coordinates.add(point);
	}

	public void addCoordinates(List<Point> points) {
		if (this.coordinates == null) {
			this.coordinates = new ArrayList<Point>();
		}
		this.coordinates.addAll(points);
	}

	public String getDistrict() {
		return district;
	}

	public void setDistrict(String district) {
		this.district = district;
	}

	public Object getValue() {
		return this.value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public abstract void addValue(Object value);

	public ValenciaItemType getType() {
		return type;
	}

	public void setType(ValenciaItemType type) {
		this.type = type;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	@Override
	public String toString() {
		return "ValenciaItemA [id=" + id + ", adminLevel=" + adminLevel + ", district=" + district + ", update="
				+ update + ", timestamp=" + timestamp + ", type=" + type + ", value=" + value + ", coordinates="
				+ coordinates + "]";
	}
}

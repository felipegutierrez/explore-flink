package org.sense.flink.util;

/**
 * This class was developed based on the metadata available at
 * http://gobiernoabierto.valencia.es/en/dataset/?id=estado-trafico-tiempo-real
 * 
 * @author felipe
 *
 */
public class TrafficStatus {
	public final static Integer STATUS_FLOWING = 0;
	public final static Integer STATUS_DENSE = 1;
	public final static Integer STATUS_CONGESTED = 2;
	public final static Integer STATUS_CHOPPED = 3;

	public static String getStatus(Integer status) {
		if (status == null) {
			return "";
		} else if (status.equals(STATUS_FLOWING)) {
			return "flowing";
		} else if (status.equals(STATUS_DENSE)) {
			return "dense";
		} else if (status.equals(STATUS_CONGESTED)) {
			return "congested";
		} else if (status.equals(STATUS_CHOPPED)) {
			return "chopped up";
		}
		return "";
	}
}

package org.sense.flink.util;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class ValenciaDistrictsTest extends TestCase {
	public ValenciaDistrictsTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(ValenciaDistrictsTest.class);
	}

	public void testDistrictId() {
		Integer id = ValenciaDistricts.getDistrictId("Ciutat Vella");
		assertEquals(Integer.valueOf(1), id);
		id = ValenciaDistricts.getDistrictId("");
		assertEquals(Integer.valueOf(0), id);
		id = ValenciaDistricts.getDistrictId("Ciutat       ");
		assertEquals(Integer.valueOf(1), id);
		id = ValenciaDistricts.getDistrictId("Pla del Real");
		assertEquals(Integer.valueOf(6), id);
		id = ValenciaDistricts.getDistrictId("Benicalap");
		assertEquals(Integer.valueOf(16), id);

		id = ValenciaDistricts.getDistrictId("Valencia district");
		assertEquals(Integer.valueOf(0), id);
	}
}

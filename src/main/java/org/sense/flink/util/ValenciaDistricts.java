package org.sense.flink.util;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

public class ValenciaDistricts {
	public static final String[] DISTRICTS = { "", "Ciutat Vella", "Eixample", "Extramurs", "Campanar", "La Saïdia",
			"Pla del Real", "Olivereta", "Patraix", "Jesús", "Quatre Carreres", "Poblats Marítims", "Camins del Grau",
			"Algirós", "Benimaclet", "Rascanya", "Benicalap" };

	public static Integer getDistrictId(String district) {
		district = district.trim();
		if (Strings.isNullOrEmpty(district)) {
			return 0;
		}
		for (int i = 0; i < DISTRICTS.length; i++) {
			if (DISTRICTS[i].toUpperCase().contains(district.toUpperCase())) {
				return i;
			}
		}
		return 0;
	}
}

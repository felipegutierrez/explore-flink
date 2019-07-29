package org.sense.flink.util;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

public class ValenciaDistricts {
	public static final String[] DISTRICTS = { "", "Ciutat Vella", "l'Eixample|Eixample", "Extramurs", "Campanar",
			"La Saïdia|La Saidia", "Pla del Real", "Olivereta", "Patraix", "Jesús|Jesus", "Quatre Carreres",
			"Poblats Marítims|Poblats Maritims", "Camins del Grau|Camins al Grau", "Algirós|Algiros", "Benimaclet",
			"Rascanya", "Benicalap" };

	public static Integer getDistrictId(String district) {
		district = district.trim();
		if (Strings.isNullOrEmpty(district)) {
			return 0;
		}
		for (int i = 0; i < DISTRICTS.length; i++) {
			String[] districtNameOptions = DISTRICTS[i].split("\\|");
			for (int j = 0; j < districtNameOptions.length; j++) {
				if (districtNameOptions[j].toUpperCase().contains(district.toUpperCase())) {
					return i;
				}
			}
		}
		return 0;
	}
}

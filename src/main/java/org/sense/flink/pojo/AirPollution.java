package org.sense.flink.pojo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.flink.shaded.guava18.com.google.common.base.Strings;

public class AirPollution implements Serializable {
	private static final long serialVersionUID = 7224111840363112436L;
	private Double so2;
	private Double co;
	private Double ozono;
	private Double nox;
	private Double no;
	private Double no2;
	private Double benc;
	private Double tolue;
	private Double xilen;
	private Double pm10;
	private Double pm2_5;
	private Double spl;

	public AirPollution() {
	}

	public AirPollution(Double so2, Double co, Double ozono, Double nox, Double no, Double no2, Double benc,
			Double tolue, Double xilen, Double pm10, Double pm2_5, Double spl) {
		this.so2 = so2;
		this.co = co;
		this.ozono = ozono;
		this.nox = nox;
		this.no = no;
		this.no2 = no2;
		this.benc = benc;
		this.tolue = tolue;
		this.xilen = xilen;
		this.pm10 = pm10;
		this.pm2_5 = pm2_5;
		this.spl = spl;
	}

	@Override
	public String toString() {
		return "AirPollution [so2=" + so2 + ", co=" + co + ", ozono=" + ozono + ", nox=" + nox + ", no=" + no + ", no2="
				+ no2 + ", benc=" + benc + ", tolue=" + tolue + ", xilen=" + xilen + ", pm10=" + pm10 + ", pm2_5="
				+ pm2_5 + ", spl=" + spl + "]";
	}

	public static AirPollution extract(String uri) {
		if (Strings.isNullOrEmpty(uri)) {
			return null;
		}
		try {
			URL url = new URL(uri);

			while (true) {
				InputStream is = url.openStream();
				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
				// StringBuilder builder = new StringBuilder();
				String line;
				int count = 0;

				// @formatter:off
				Double so2_sum = 0.0;
				Double co_sum = 0.0;
				Double ozono_sum = 0.0;
				Double nox_sum = 0.0;
				Double no_sum = 0.0;
				Double no2_sum = 0.0;
				Double benc_sum = 0.0;
				Double tolue_sum = 0.0;
				Double xilen_sum = 0.0;
				Double pm10_sum = 0.0;
				Double pm2_5_sum = 0.0;
				Double spl_sum = 0.0;
				
				while ((line = bufferedReader.readLine()) != null) {
					if (count != 0) {
						// builder.append(line + "\n");
						String[] values = line.split(";");
						if (values.length > 0) {
							so2_sum = so2_sum + Double.parseDouble(Strings.isNullOrEmpty(values[1]) ? "0.0" : values[1].replace(",", "."));
						}
						if (values.length > 1) {
							co_sum = co_sum + Double.parseDouble(Strings.isNullOrEmpty(values[2]) ? "0.0" : values[2].replace(",", "."));
						}
						if (values.length > 3) {
							ozono_sum = ozono_sum + Double.parseDouble(Strings.isNullOrEmpty(values[3]) ? "0.0" : values[3].replace(",", "."));
						}
						if (values.length > 4) {
							nox_sum = nox_sum + Double.parseDouble(Strings.isNullOrEmpty(values[4]) ? "0.0" : values[4].replace(",", "."));
						}
						if (values.length > 5) {
							no_sum = no_sum + Double.parseDouble(Strings.isNullOrEmpty(values[5]) ? "0.0" : values[5].replace(",", "."));
						}
						if (values.length > 6) {
							no2_sum = no2_sum + Double.parseDouble(Strings.isNullOrEmpty(values[6]) ? "0.0" : values[6].replace(",", "."));
						}
						if (values.length > 7) {
							benc_sum = benc_sum + Double.parseDouble(Strings.isNullOrEmpty(values[7]) ? "0.0" : values[7].replace(",", "."));
						}
						if (values.length > 8) {
							tolue_sum = tolue_sum + Double.parseDouble(Strings.isNullOrEmpty(values[8]) ? "0.0" : values[8].replace(",", "."));
						}
						if (values.length > 9) {
							xilen_sum = xilen_sum + Double.parseDouble(Strings.isNullOrEmpty(values[9]) ? "0.0" : values[9].replace(",", "."));
						}
						if (values.length > 10) {
							pm10_sum = pm10_sum + Double.parseDouble(Strings.isNullOrEmpty(values[10]) ? "0.0" : values[10].replace(",", "."));
						}
						if (values.length > 11) {
							pm2_5_sum = pm2_5_sum + Double.parseDouble(Strings.isNullOrEmpty(values[11]) ? "0.0" : values[11].replace(",", "."));
						}
						if (values.length > 12) {
							spl_sum = spl_sum + Double.parseDouble(Strings.isNullOrEmpty(values[12]) ? "0.0" : values[12].replace(",", "."));
						}
					}
					count++;
				}
				count--;
				bufferedReader.close();
				AirPollution airPollution = new AirPollution(so2_sum / 2, co_sum / 2, ozono_sum / 2, nox_sum / 2,
						no_sum / 2, no2_sum / 2, benc_sum / 2, tolue_sum / 2, xilen_sum / 2, pm10_sum / 2,
						pm2_5_sum / 2, spl_sum / 2);
				return airPollution;
				// builder.toString();
				// @formatter:on
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}
}

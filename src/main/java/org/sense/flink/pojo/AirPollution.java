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

	/**
	 * Constructor with default values
	 */
	public AirPollution() {
		// normal environment
		// this(3.0, 0.1, 47.0, 33.0, 7.0, 22.0, 0.0, 0.0, 0.0, 11.0, 5.0, 60.0);
		// threshold for a acceptable environment
		this(15.0, 15.0, 50.0, 35.0, 30.0, 60.0, 60.0, 75.0, 60.0, 13.0, 7.0, 70.0);
		// very clean environment
		// this(2.0, 0.1, 40.0, 30.0, 5.0, 19.0, 0.0, 0.0, 0.0, 9.0, 3.0, 50.0);
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

	public Double getSo2() {
		return so2;
	}

	public void setSo2(Double so2) {
		this.so2 = so2;
	}

	public Double getCo() {
		return co;
	}

	public void setCo(Double co) {
		this.co = co;
	}

	public Double getOzono() {
		return ozono;
	}

	public void setOzono(Double ozono) {
		this.ozono = ozono;
	}

	public Double getNox() {
		return nox;
	}

	public void setNox(Double nox) {
		this.nox = nox;
	}

	public Double getNo() {
		return no;
	}

	public void setNo(Double no) {
		this.no = no;
	}

	public Double getNo2() {
		return no2;
	}

	public void setNo2(Double no2) {
		this.no2 = no2;
	}

	public Double getBenc() {
		return benc;
	}

	public void setBenc(Double benc) {
		this.benc = benc;
	}

	public Double getTolue() {
		return tolue;
	}

	public void setTolue(Double tolue) {
		this.tolue = tolue;
	}

	public Double getXilen() {
		return xilen;
	}

	public void setXilen(Double xilen) {
		this.xilen = xilen;
	}

	public Double getPm10() {
		return pm10;
	}

	public void setPm10(Double pm10) {
		this.pm10 = pm10;
	}

	public Double getPm2_5() {
		return pm2_5;
	}

	public void setPm2_5(Double pm2_5) {
		this.pm2_5 = pm2_5;
	}

	public Double getSpl() {
		return spl;
	}

	public void setSpl(Double spl) {
		this.spl = spl;
	}

	public void compareAirPollution(AirPollution threshold) {
		// @formatter:off
		if (threshold == null) {
			return;
		}
		if (this.so2 != null && this.so2.doubleValue() >= threshold.so2.doubleValue()) {
			System.out.println("SO2(µg/m³) [" + this.so2.doubleValue() + "] exceeded he threshold [" + threshold.so2.doubleValue() + "]");
		}
		if (this.co != null && this.co.doubleValue() >= threshold.co.doubleValue()) {
			System.out.println("CO(mg/m³) [" + this.co.doubleValue() + "] exceeded he threshold [" + threshold.co.doubleValue() + "]");
		}
		if (this.ozono != null && this.ozono.doubleValue() >= threshold.ozono.doubleValue()) {
			System.out.println("Ozono(µg/m³) [" + this.ozono.doubleValue() + "] exceeded he threshold [" + threshold.ozono.doubleValue() + "]");
		}
		if (this.nox != null && this.nox.doubleValue() >= threshold.nox.doubleValue()) {
			System.out.println("NOx(µg/m³) [" + this.nox.doubleValue() + "] exceeded he threshold [" + threshold.nox.doubleValue() + "]");
		}
		if (this.no != null && this.no.doubleValue() >= threshold.no.doubleValue()) {
			System.out.println("NO(µg/m³) [" + this.no.doubleValue() + "] exceeded he threshold [" + threshold.no.doubleValue() + "]");
		}
		if (this.no2 != null && this.no2.doubleValue() >= threshold.no2.doubleValue()) {
			System.out.println("NO2(µg/m³) [" + this.no2.doubleValue() + "] exceeded he threshold [" + threshold.no2.doubleValue() + "]");
		}
		if (this.benc != null && this.benc.doubleValue() >= threshold.benc.doubleValue()) {
			System.out.println("Benceno(µg/m³) [" + this.benc.doubleValue() + "] exceeded he threshold [" + threshold.benc.doubleValue() + "]");
		}
		if (this.tolue != null && this.tolue.doubleValue() >= threshold.tolue.doubleValue()) {
			System.out.println("Tolueno(µg/m³) [" + this.tolue.doubleValue() + "] exceeded he threshold [" + threshold.tolue.doubleValue() + "]");
		}
		if (this.xilen != null && this.xilen.doubleValue() >= threshold.xilen.doubleValue()) {
			System.out.println("Xileno(µg/m³) [" + this.xilen.doubleValue() + "] exceeded he threshold [" + threshold.xilen.doubleValue() + "]");
		}
		if (this.pm10 != null && this.pm10.doubleValue() >= threshold.pm10.doubleValue()) {
			System.out.println("PM10(µg/m³) [" + this.pm10.doubleValue() + "] exceeded he threshold [" + threshold.pm10.doubleValue() + "]");
		}
		if (this.pm2_5 != null && this.pm2_5.doubleValue() >= threshold.pm2_5.doubleValue()) {
			System.out.println("PM2,5(µg/m³) [" + this.pm2_5.doubleValue() + "] exceeded he threshold [" + threshold.pm2_5.doubleValue() + "]");
		}
		if (this.spl != null && this.spl.doubleValue() >= threshold.spl.doubleValue()) {
			System.out.println("Ruido(dBA) [" + this.spl.doubleValue() + "] exceeded he threshold [" + threshold.spl.doubleValue() + "]");
		}
		// @formatter:on
	}

	@Override
	public String toString() {
		return "AirPollution [SO2(µg/m³)=" + so2 + ", CO(mg/m³)=" + co + ", Ozono(µg/m³)=" + ozono + ", NOx(µg/m³)="
				+ nox + ", NO(µg/m³)=" + no + ", NO2(µg/m³)=" + no2 + ", Benceno(µg/m³)=" + benc + ", Tolueno(µg/m³)="
				+ tolue + ", Xileno(µg/m³)=" + xilen + ", PM10(µg/m³)=" + pm10 + ", PM2,5(µg/m³)=" + pm2_5
				+ ", Ruido(dBA)=" + spl + "]";
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
				AirPollution airPollution = new AirPollution(so2_sum / count, co_sum / count, ozono_sum / count, nox_sum / count,
						no_sum / count, no2_sum / count, benc_sum / count, tolue_sum / count, xilen_sum / count, pm10_sum / count,
						pm2_5_sum / count, spl_sum / count);
				return airPollution;
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

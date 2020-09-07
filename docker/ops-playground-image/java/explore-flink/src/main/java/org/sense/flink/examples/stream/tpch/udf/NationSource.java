package org.sense.flink.examples.stream.tpch.udf;

import static org.sense.flink.util.MetricLabels.TPCH_DATA_NATION;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.sense.flink.examples.stream.tpch.pojo.Nation;

public class NationSource {
	private final String dataFilePath;

	public NationSource() {
		this(TPCH_DATA_NATION);
	}

	public NationSource(String dataFilePath) {
		this.dataFilePath = dataFilePath;
	}

	public List<Nation> getNations() {
		String line = null;
		List<Nation> nationList = new ArrayList<Nation>();
		long rowNumber = 0;
		try {
			InputStream s = new FileInputStream(this.dataFilePath);
			BufferedReader r = new BufferedReader(new InputStreamReader(s, StandardCharsets.UTF_8));

			line = r.readLine();
			while (line != null) {
				rowNumber++;
				nationList.add(getNationItem(line, rowNumber));
				line = r.readLine();
			}
			r.close();
			r = null;
			s.close();
			s = null;
		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		} catch (Exception e) {
			throw new RuntimeException("Invalid record: " + line, e);
		}
		return nationList;
	}

	private Nation getNationItem(String line, long rowNumber) {
		String[] tokens = line.split("\\|");
		if (tokens.length != 4) {
			throw new RuntimeException("Invalid record: " + line);
		}
		Nation nation;
		try {
			long nationKey = Long.parseLong(tokens[0]);
			String name = tokens[1];
			long regionKey = Long.parseLong(tokens[2]);
			String comment = tokens[3];
			nation = new Nation(rowNumber, nationKey, name, regionKey, comment);
		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		} catch (Exception e) {
			throw new RuntimeException("Invalid record: " + line, e);
		}
		return nation;
	}
}

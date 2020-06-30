package org.sense.flink.examples.stream.tpch.udf;

import static org.sense.flink.util.MetricLabels.TPCH_DATA_NATION;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
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
		InputStream s = null;
		BufferedReader r = null;
		List<Nation> nationList = new ArrayList<Nation>();
		long rowNumber = 0;
		try {
			s = new FileInputStream(this.dataFilePath);
			r = new BufferedReader(new InputStreamReader(s, StandardCharsets.UTF_8));

			while (r.ready() && (line = r.readLine()) != null) {
				rowNumber++;
				nationList.add(getNationItem(line, rowNumber));
			}
		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		} catch (Exception e) {
			throw new RuntimeException("Invalid record: " + line, e);
		} finally {

		}
		try {
			r.close();
			r = null;
			s.close();
			s = null;
		} catch (IOException e) {
			e.printStackTrace();
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

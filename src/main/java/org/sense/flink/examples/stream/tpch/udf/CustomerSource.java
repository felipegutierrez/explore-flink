package org.sense.flink.examples.stream.tpch.udf;

import static org.sense.flink.util.MetricLabels.TPCH_DATA_COSTUMER;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.sense.flink.examples.stream.tpch.pojo.Customer;
import org.sense.flink.examples.stream.tpch.pojo.Nation;
import org.sense.flink.util.DataRateListener;

public class CustomerSource extends RichSourceFunction<Customer> {

	private static final long serialVersionUID = 1L;

	private final String dataFilePath;
	private DataRateListener dataRateListener;
	private boolean running;

	public CustomerSource() {
		this(TPCH_DATA_COSTUMER);
	}

	public CustomerSource(String dataFilePath) {
		this.running = true;
		this.dataFilePath = dataFilePath;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.dataRateListener = new DataRateListener();
		this.dataRateListener.start();
	}

	@Override
	public void run(SourceContext<Customer> sourceContext) {
		try {
			while (running) {
				generateCostumer(sourceContext);
				Thread.sleep(2 * 1000);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void generateCostumer(SourceContext<Customer> sourceContext) {
		try {
			InputStream stream = new FileInputStream(dataFilePath);
			BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));

			int rowNumber = 0;
			long startTime = System.nanoTime();
			String line = reader.readLine();
			while (line != null) {
				rowNumber++;
				sourceContext.collect(getCustomerItem(line, rowNumber));

				// sleep in nanoseconds to have a reproducible data rate for the data source
				this.dataRateListener.busySleep(startTime);

				// get start time and line for the next iteration
				startTime = System.nanoTime();
				line = reader.readLine();
			}
			reader.close();
			reader = null;
			stream.close();
			stream = null;
		} catch (FileNotFoundException e) {
			System.err.println("Please make sure they are available at [" + dataFilePath + "].");
			System.err.println(
					" Follow the instructions at [https://docs.deistercloud.com/content/Databases.30/TPCH%20Benchmark.90/Data%20generation%20tool.30.xml?embedded=true] in order to download and create them.");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Customer getCustomerItem(String line, int rowNumber) {
		String[] tokens = line.split("\\|");
		if (tokens.length != 8) {
			throw new RuntimeException("Invalid record: " + line);
		}
		Customer customer;
		try {
			long customerKey = Long.parseLong(tokens[0]);
			String name = tokens[1];
			String address = tokens[2];
			long nationKey = Long.parseLong(tokens[3]);
			String phone = tokens[4];
			long accountBalance = (long) Double.parseDouble(tokens[5]);
			String marketSegment = tokens[6];
			String comment = tokens[7];

			customer = new Customer(rowNumber, customerKey, name, address, nationKey, phone, accountBalance,
					marketSegment, comment);
		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		} catch (Exception e) {
			throw new RuntimeException("Invalid record: " + line, e);
		}
		return customer;
	}

	public List<Customer> getCustomers() {
		String line = null;
		List<Customer> customerList = new ArrayList<Customer>();
		try {
			InputStream s = new FileInputStream(TPCH_DATA_COSTUMER);
			BufferedReader r = new BufferedReader(new InputStreamReader(s, StandardCharsets.UTF_8));
			int rowNumber = 0;
			line = r.readLine();
			while (line != null) {
				rowNumber++;
				customerList.add(getCustomerItem(line, rowNumber));
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
		return customerList;
	}

	public List<Long> getCustomersKeys() {
		String line = null;
		List<Long> customerKeyList = new ArrayList<Long>();
		try {
			InputStream s = new FileInputStream(TPCH_DATA_COSTUMER);
			BufferedReader r = new BufferedReader(new InputStreamReader(s, StandardCharsets.UTF_8));
			int rowNumber = 0;
			line = r.readLine();
			while (line != null) {
				rowNumber++;
				customerKeyList.add(getCustomerItem(line, rowNumber).getCustomerKey());
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
		return customerKeyList;
	}

	public Map<Integer, Tuple4<String, String, String, Double>> getCustomersWithNation() {
		NationSource nationSource = new NationSource();
		List<Nation> nations = nationSource.getNations();
		List<Customer> customers = getCustomers();
		Map<Integer, Tuple4<String, String, String, Double>> customersWithNation = new HashMap<Integer, Tuple4<String, String, String, Double>>();

		for (Nation nation : nations) {
			for (Customer customer : customers) {
				if (customer.getNationKey() == nation.getNationKey()) {
					Integer customerRow = (int) customer.getRowNumber();
					String customerKey = String.valueOf(customer.getCustomerKey());
					String customerName = customer.getName();
					String nationName = nation.getName();
					Double customerAccountBalance = customer.getAccountBalance();

					customersWithNation.put(customerRow,
							Tuple4.of(customerKey, customerName, nationName, customerAccountBalance));
				}
			}
		}
		return customersWithNation;
	}

	@Override
	public void cancel() {
		this.running = false;
	}
}

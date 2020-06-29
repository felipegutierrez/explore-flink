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
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.sense.flink.examples.stream.tpch.pojo.Customer;
import org.sense.flink.util.DataRateListener;

public class CustomerSource extends RichSourceFunction<Customer> {

	private static final long serialVersionUID = 1L;

	private final String dataFilePath;
	private DataRateListener dataRateListener;
	private boolean running;
	private transient BufferedReader reader;
	private transient InputStream stream;

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
	public void run(SourceContext<Customer> sourceContext) throws Exception {
		while (running) {
			generateCostumerItemArray(sourceContext);
		}
		this.reader.close();
		this.reader = null;
		this.stream.close();
		this.stream = null;
	}

	private void generateCostumerItemArray(SourceContext<Customer> sourceContext) {
		try {
			stream = new FileInputStream(dataFilePath);
			reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
			String line;
			Customer customerItem;
			long startTime;
			while (reader.ready() && (line = reader.readLine()) != null) {
				startTime = System.nanoTime();
				customerItem = getCustomerItem(line);
				sourceContext.collectWithTimestamp(customerItem, getEventTime(customerItem));

				// sleep in nanoseconds to have a reproducible data rate for the data source
				this.dataRateListener.busySleep(startTime);
			}
		} catch (FileNotFoundException e) {
			System.err.println("Please make sure they are available at [" + dataFilePath + "].");
			System.err.println(
					" Follow the instructions at [https://docs.deistercloud.com/content/Databases.30/TPCH%20Benchmark.90/Data%20generation%20tool.30.xml?embedded=true] in order to download and create them.");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Customer getCustomerItem(String line) {
		String[] tokens = line.split("\\|");
		if (tokens.length != 8) {
			throw new RuntimeException("Invalid record: " + line);
		}
		Customer customer;
		try {
			int rowNumber = Integer.parseInt(tokens[0]);
			long customerKey = Long.parseLong(tokens[1].split("#")[1]);
			String name = tokens[2];
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
		InputStream s = null;
		BufferedReader r = null;
		List<Customer> customerList = new ArrayList<Customer>();
		try {
			s = new FileInputStream(TPCH_DATA_COSTUMER);
			r = new BufferedReader(new InputStreamReader(s, StandardCharsets.UTF_8));

			while (r.ready() && (line = r.readLine()) != null) {
				customerList.add(getCustomerItem(line));
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
		return customerList;
	}

	public long getEventTime(Customer value) {
		return value.getTimestamp();
	}

	@Override
	public void cancel() {
		try {
			this.running = false;
			if (this.reader != null) {
				this.reader.close();
			}
			if (this.stream != null) {
				this.stream.close();
			}
		} catch (IOException ioe) {
			throw new RuntimeException("Could not cancel SourceFunction", ioe);
		} finally {
			this.reader = null;
			this.stream = null;
		}
	}
}

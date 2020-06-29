package org.sense.flink.examples.stream.tpch.udf;

import static org.sense.flink.util.MetricLabels.TPCH_DATA_ORDER;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.sense.flink.examples.stream.tpch.pojo.Order;
import org.sense.flink.util.DataRateListener;

public class OrdersSource extends RichSourceFunction<Order> {

	private static final long serialVersionUID = 1L;

	public static final transient DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	private final String dataFilePath;
	private DataRateListener dataRateListener;
	private boolean running;
	private transient BufferedReader reader;
	private transient InputStream stream;

	public OrdersSource() {
		this(TPCH_DATA_ORDER);
	}

	public OrdersSource(String dataFilePath) {
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
	public void run(SourceContext<Order> sourceContext) throws Exception {
		while (running) {
			generateOrderItemArray(sourceContext);
		}
		this.reader.close();
		this.reader = null;
		this.stream.close();
		this.stream = null;
	}

	private void generateOrderItemArray(SourceContext<Order> sourceContext) {
		try {
			stream = new FileInputStream(dataFilePath);
			reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
			String line;
			Order orderItem;
			int rowNumber = 0;
			long startTime;
			while (reader.ready() && (line = reader.readLine()) != null) {
				startTime = System.nanoTime();
				rowNumber++;
				orderItem = getOrderItem(line, rowNumber);
				sourceContext.collectWithTimestamp(orderItem, getEventTime(orderItem));

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

	private Order getOrderItem(String line, int rowNumber) {
		String[] tokens = line.split("\\|");
		if (tokens.length != 9) {
			throw new RuntimeException("Invalid record: " + line);
		}
		Order order;
		try {
			long orderKey = Long.parseLong(tokens[0]);
			long customerKey = Long.parseLong(tokens[1]);
			char orderStatus = tokens[2].charAt(0);
			long totalPrice = (long) Double.parseDouble(tokens[3]);
			int orderDate = Integer.parseInt(tokens[4].replace("-", ""));
			String orderPriority = tokens[5];
			String clerk = tokens[6];
			int shipPriority = Integer.parseInt(tokens[7]);
			String comment = tokens[8];
			order = new Order(rowNumber, orderKey, customerKey, orderStatus, totalPrice, orderDate, orderPriority,
					clerk, shipPriority, comment);
		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		} catch (Exception e) {
			throw new RuntimeException("Invalid record: " + line, e);
		}
		return order;
	}

	public static String format(int date) {
		String value = String.valueOf(date);
		return value.substring(0, 4) + "-" + value.substring(4, 6) + "-" + value.substring(6, 8);
	}

	public long getEventTime(Order value) {
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

	public static void main(String[] args) {

		System.out.println(OrdersSource.format(19961102));
	}
}

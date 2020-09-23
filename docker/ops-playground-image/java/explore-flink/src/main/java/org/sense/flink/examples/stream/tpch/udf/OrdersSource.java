package org.sense.flink.examples.stream.tpch.udf;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.sense.flink.examples.stream.tpch.pojo.Order;
import org.sense.flink.util.DataRateListener;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import static org.sense.flink.util.MetricLabels.TPCH_DATA_ORDER;

public class OrdersSource extends RichSourceFunction<Order> {

    public static final transient DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    private static final long serialVersionUID = 1L;
    private final String dataFilePath;
    private final long maxCount;
    private DataRateListener dataRateListener;
    private boolean running;

    public OrdersSource() {
        // maxCount = -1 means that we are going to generate data forever
        this(TPCH_DATA_ORDER, -1);
    }

    public OrdersSource(long maxCount) {
        this(TPCH_DATA_ORDER, maxCount);
    }

    public OrdersSource(String dataFilePath, long maxCount) {
        this.running = true;
        this.maxCount = maxCount;
        if (dataFilePath == null) {
            this.dataFilePath = TPCH_DATA_ORDER;
        } else {
            this.dataFilePath = dataFilePath;
        }
    }

    public static String format(int date) {
        String value = String.valueOf(date);
        return value.substring(0, 4) + "-" + value.substring(4, 6) + "-" + value.substring(6, 8);
    }

    public static int getYear(int date) {
        String value = String.valueOf(date);
        return Integer.parseInt(value.substring(0, 4));
    }

    public static void main(String[] args) {

        System.out.println(OrdersSource.format(19961102));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.dataRateListener = new DataRateListener();
        this.dataRateListener.start();
    }

    @Override
    public void run(SourceContext<Order> sourceContext) {
        try {
            long count = 0;
            while (running) {
                count++;
                System.out.println("Reading Order source table for the [" + count + "] time.");

                generateOrderItem(sourceContext);

                Thread.sleep(2 * 1000);
                // decide when to stop generate data
                if (this.maxCount != -1 && count >= this.maxCount) {
                    this.running = false;
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void generateOrderItem(SourceContext<Order> sourceContext) {
        try {
            System.out.println("reading file: " + dataFilePath);
            InputStream stream = new FileInputStream(dataFilePath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));

            int rowNumber = 0;
            long startTime = System.nanoTime();
            String line = reader.readLine();
            while (line != null) {
                rowNumber++;
                sourceContext.collect(getOrderItem(line, rowNumber));

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

    @Override
    public void cancel() {
        this.running = false;
    }
}

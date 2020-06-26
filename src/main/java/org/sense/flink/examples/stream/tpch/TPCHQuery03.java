package org.sense.flink.examples.stream.tpch;

import static org.sense.flink.util.MetricLabels.OPERATOR_SINK;
import static org.sense.flink.util.MetricLabels.OPERATOR_SOURCE;
import static org.sense.flink.util.MetricLabels.SINK_DATA_MQTT;
import static org.sense.flink.util.MetricLabels.SINK_TEXT;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.sense.flink.examples.stream.tpch.pojo.Customer;
import org.sense.flink.examples.stream.tpch.pojo.LineItem;
import org.sense.flink.examples.stream.tpch.pojo.Order;

/**
 * Implementation of the TPC-H Benchmark query 03 also available at:
 * 
 * https://github.com/apache/flink/blob/master/flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java/relational/TPCHQuery3.java
 * 
 * The original query can be found at:
 * 
 * https://docs.deistercloud.com/content/Databases.30/TPCH%20Benchmark.90/Sample%20querys.20.xml
 * 
 * 
 * @author Felipe Oliveira Gutierrez
 *
 */
public class TPCHQuery03 {

	public static void main(String[] args) throws Exception {
		new TPCHQuery03();
	}

	public TPCHQuery03() throws Exception {
		this(SINK_TEXT);
	}

	public TPCHQuery03(String output) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStateBackend(new FsStateBackend("file:///tmp/flink/checkpoints"));
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		DataStream<Order> orders = env.addSource(new OrdersSource())
				.name(OPERATOR_SOURCE + OrdersSource.class.getSimpleName())
				.uid(OPERATOR_SOURCE + OrdersSource.class.getSimpleName())
				.assignTimestampsAndWatermarks(new OrderTimestampAndWatermarkAssigner());

		// Filter market segment "AUTOMOBILE"
		// customers = customers.filter(new CustomerFilter());

		// Filter all Orders with o_orderdate < 12.03.1995
		// orders = orders.filter(new OrderFilter());

		// @formatter:off
		DataStream<ShippingPriorityItem> shippingPriorityStream01 = orders
				.keyBy(new OrderCustomerKeySelector())
				.timeWindow(Time.seconds(10))
				.aggregate(new OrderCustomerAggregator(), new OrderProcessWindowFunction());
		// shippingPriorityStream01.print();
		DataStream<ShippingPriorityItem> shippingPriorityStream02 = shippingPriorityStream01
				.keyBy(new ShippingPriorityKeySelector())
				.timeWindow(Time.seconds(60))
				.aggregate(new ShippingPriorityItemAggregator(), new ShippingPriorityProcessWindowFunction());
		shippingPriorityStream02.print();
		
		if (output.equalsIgnoreCase(SINK_DATA_MQTT)) {
			shippingPriorityStream02.print().name(OPERATOR_SINK).uid(OPERATOR_SINK);
		} else if (output.equalsIgnoreCase(SINK_TEXT)) {
			shippingPriorityStream02.print().name(OPERATOR_SINK).uid(OPERATOR_SINK);
		} else {
			System.out.println("discarding output");
		}
		// @formatter:on

		System.out.println("Execution plan >>>\n" + env.getExecutionPlan());
		env.execute(TPCHQuery03.class.getSimpleName());
	}

	private static class OrderCustomerAggregator implements AggregateFunction<Order, List<Order>, List<Order>> {
		private static final long serialVersionUID = 1L;

		@Override
		public List<Order> createAccumulator() {
			return new ArrayList<Order>();
		}

		@Override
		public List<Order> add(Order value, List<Order> accumulator) {
			accumulator.add(value);
			return accumulator;
		}

		@Override
		public List<Order> getResult(List<Order> accumulator) {
			return accumulator;
		}

		@Override
		public List<Order> merge(List<Order> a, List<Order> b) {
			a.addAll(b);
			return a;
		}

	}

	private static class OrderProcessWindowFunction
			extends ProcessWindowFunction<List<Order>, ShippingPriorityItem, Long, TimeWindow> {
		private static final long serialVersionUID = 1L;

		private ListState<Customer> customerList = null;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			ListStateDescriptor<Customer> customerDescriptor = new ListStateDescriptor<Customer>("customerState",
					Customer.class);
			customerList = getRuntimeContext().getListState(customerDescriptor);
		}

		@Override
		public void process(Long key,
				ProcessWindowFunction<List<Order>, ShippingPriorityItem, Long, TimeWindow>.Context context,
				Iterable<List<Order>> input, Collector<ShippingPriorityItem> out) throws Exception {

			if (customerList != null && Iterators.size(customerList.get().iterator()) == 0) {
				CustomerSource customerSource = new CustomerSource();
				List<Customer> customers = customerSource.getCustomers();
				customerList.addAll(customers);
			}

			for (Customer customer : customerList.get()) {
				for (Iterator<List<Order>> iterator = input.iterator(); iterator.hasNext();) {
					List<Order> orders = (List<Order>) iterator.next();

					for (Order order : orders) {
						if (order.getCustomerKey() == customer.getCustomerKey()) {
							try {
								out.collect(new ShippingPriorityItem(order.getOrderKey(), 0.0,
										OrdersSource.format(order.getOrderDate()), order.getShipPriority()));
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				}
			}
		}
	}

	private static class ShippingPriorityItemAggregator
			implements AggregateFunction<ShippingPriorityItem, List<ShippingPriorityItem>, List<ShippingPriorityItem>> {
		private static final long serialVersionUID = 1L;

		@Override
		public List<ShippingPriorityItem> createAccumulator() {
			return new ArrayList<ShippingPriorityItem>();
		}

		@Override
		public List<ShippingPriorityItem> add(ShippingPriorityItem value, List<ShippingPriorityItem> accumulator) {
			accumulator.add(value);
			return accumulator;
		}

		@Override
		public List<ShippingPriorityItem> getResult(List<ShippingPriorityItem> accumulator) {
			return accumulator;
		}

		@Override
		public List<ShippingPriorityItem> merge(List<ShippingPriorityItem> a, List<ShippingPriorityItem> b) {
			a.addAll(b);
			return a;
		}
	}

	private static class ShippingPriorityProcessWindowFunction
			extends ProcessWindowFunction<List<ShippingPriorityItem>, ShippingPriorityItem, Long, TimeWindow> {
		private static final long serialVersionUID = 1L;

		private ListState<LineItem> lineItemList = null;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			ListStateDescriptor<LineItem> lineItemDescriptor = new ListStateDescriptor<LineItem>("lineItemState",
					LineItem.class);
			lineItemList = getRuntimeContext().getListState(lineItemDescriptor);
		}

		@Override
		public void process(Long key,
				ProcessWindowFunction<List<ShippingPriorityItem>, ShippingPriorityItem, Long, TimeWindow>.Context context,
				Iterable<List<ShippingPriorityItem>> input, Collector<ShippingPriorityItem> out) throws Exception {
			if (lineItemList != null && Iterators.size(lineItemList.get().iterator()) == 0) {
				LineItemSource lineItemSource = new LineItemSource();
				List<LineItem> lineItems = lineItemSource.getLineItems();
				lineItemList.addAll(lineItems);
			}

			for (LineItem lineItem : lineItemList.get()) {
				for (Iterator<List<ShippingPriorityItem>> iterator = input.iterator(); iterator.hasNext();) {
					List<ShippingPriorityItem> shippingPriorityItemList = (List<ShippingPriorityItem>) iterator.next();

					for (ShippingPriorityItem shippingPriorityItem : shippingPriorityItemList) {
						if (shippingPriorityItem.getOrderkey().equals(lineItem.getRowNumber())) {
							out.collect(shippingPriorityItem);
						}
					}
				}
			}
		}
	}

	private static class OrderTimestampAndWatermarkAssigner extends AscendingTimestampExtractor<Order> {
		private static final long serialVersionUID = 1L;

		@Override
		public long extractAscendingTimestamp(Order element) {
			return element.getTimestamp();
		}
	}

	private static class CustomerFilter implements FilterFunction<Customer> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Customer c) {
			return c.getMarketSegment().equals("AUTOMOBILE");
		}
	}

	private static class OrderFilter implements FilterFunction<Order> {
		private static final long serialVersionUID = 1L;
		private final DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		private final Date date;

		public OrderFilter() throws ParseException {
			date = sdf.parse("1995-03-12");
		}

		@Override
		public boolean filter(Order o) throws ParseException {
			Date orderDate = new Date(o.getOrderDate());
			return orderDate.before(date);
		}
	}

	private static class OrderCustomerKeySelector implements KeySelector<Order, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long getKey(Order value) throws Exception {
			return value.getCustomerKey();
		}
	}

	private static class ShippingPriorityKeySelector implements KeySelector<ShippingPriorityItem, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long getKey(ShippingPriorityItem value) throws Exception {
			return value.getOrderkey();
		}
	}

	private static class ShippingPriorityItem implements Serializable {
		private static final long serialVersionUID = 1L;

		private Long orderkey;
		private Double revenue;
		private String orderdate;
		private Integer shippriority;

		public ShippingPriorityItem(Long orderkey, Double revenue, String orderdate, Integer shippriority) {
			this.orderkey = orderkey;
			this.revenue = revenue;
			this.orderdate = orderdate;
			this.shippriority = shippriority;
		}

		public Long getOrderkey() {
			return orderkey;
		}

		public void setOrderkey(Long orderkey) {
			this.orderkey = orderkey;
		}

		public Double getRevenue() {
			return revenue;
		}

		public void setRevenue(Double revenue) {
			this.revenue = revenue;
		}

		public String getOrderdate() {
			return orderdate;
		}

		public void setOrderdate(String orderdate) {
			this.orderdate = orderdate;
		}

		public Integer getShippriority() {
			return shippriority;
		}

		public void setShippriority(Integer shippriority) {
			this.shippriority = shippriority;
		}

		@Override
		public String toString() {
			return "ShippingPriorityItem [getOrderkey()=" + getOrderkey() + ", getRevenue()=" + getRevenue()
					+ ", getOrderdate()=" + getOrderdate() + ", getShippriority()=" + getShippriority() + "]";
		}
	}
}

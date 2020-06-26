package org.sense.flink.examples.stream.tpch;

import static org.sense.flink.util.MetricLabels.OPERATOR_SINK;
import static org.sense.flink.util.MetricLabels.OPERATOR_SOURCE;
import static org.sense.flink.util.MetricLabels.SINK_DATA_MQTT;
import static org.sense.flink.util.MetricLabels.SINK_TEXT;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
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
		orders
			.keyBy(new OrderCustomerKeySelector())
			.timeWindow(Time.seconds(5))
			.aggregate(new OrderCustomerAggregator(), new OrderProcessWindowFunction())
			.flatMap(new ShippingPriorityItemFlatMap());
		// @formatter:on

		if (output.equalsIgnoreCase(SINK_DATA_MQTT)) {
			orders.print().name(OPERATOR_SINK).uid(OPERATOR_SINK);
		} else if (output.equalsIgnoreCase(SINK_TEXT)) {
			orders.print().name(OPERATOR_SINK).uid(OPERATOR_SINK);
		} else {
			System.out.println("discarding output");
		}

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
			extends ProcessWindowFunction<List<Order>, List<ShippingPriorityItem>, Long, TimeWindow> {
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
				ProcessWindowFunction<List<Order>, List<ShippingPriorityItem>, Long, TimeWindow>.Context context,
				Iterable<List<Order>> input, Collector<List<ShippingPriorityItem>> out) throws Exception {

			if (customerList != null && Iterators.size(customerList.get().iterator()) == 0) {
				CustomerSource customerSource = new CustomerSource();
				List<Customer> customers = customerSource.getCustomers();
				customerList.addAll(customers);
			}
			System.out.println("");

			List<ShippingPriorityItem> listShippingPriorityItem = new ArrayList<ShippingPriorityItem>();
			for (Customer customer : customerList.get()) {
				for (Iterator<List<Order>> iterator = input.iterator(); iterator.hasNext();) {
					List<Order> orders = (List<Order>) iterator.next();

					for (Order order : orders) {
						if (order.getCustomerKey() != customer.getCustomerKey()) {
							listShippingPriorityItem.add(new ShippingPriorityItem(order.getOrderKey(), 0.0,
									OrdersSource.format(order.getOrderDate()), order.getShipPriority()));
						}
					}
				}

			}
			out.collect(listShippingPriorityItem);
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

	private static class ShippingPriorityItemFlatMap implements FlatMapFunction<List<ShippingPriorityItem>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(List<ShippingPriorityItem> value, Collector<String> out) throws Exception {
			String result = null;
			for (ShippingPriorityItem shippingPriorityItem : value) {
				result += "ShippingPriorityItem [getOrderkey()=" + shippingPriorityItem.getOrderkey()
						+ ", getRevenue()=" + shippingPriorityItem.getRevenue() + ", getOrderdate()="
						+ shippingPriorityItem.getOrderdate() + ", getShippriority()="
						+ shippingPriorityItem.getShippriority() + "]";
			}
			out.collect(result);
		}
	}

	private static class OrderCustomerKeySelector implements KeySelector<Order, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long getKey(Order value) throws Exception {
			return value.getCustomerKey();
		}
	}

	private static class ShippingPriorityItem extends Tuple4<Long, Double, String, Integer> {
		private static final long serialVersionUID = 1L;

		public ShippingPriorityItem(Long orderkey, Double revenue, String orderdate, Integer shippriority) {
			this.f0 = orderkey;
			this.f1 = revenue;
			this.f2 = orderdate;
			this.f3 = shippriority;
		}

		public Long getOrderkey() {
			return this.f0;
		}

		public void setOrderkey(Long orderkey) {
			this.f0 = orderkey;
		}

		public Double getRevenue() {
			return this.f1;
		}

		public void setRevenue(Double revenue) {
			this.f1 = revenue;
		}

		public String getOrderdate() {
			return this.f2;
		}

		public Integer getShippriority() {
			return this.f3;
		}

		@Override
		public String toString() {
			return "ShippingPriorityItem [getOrderkey()=" + getOrderkey() + ", getRevenue()=" + getRevenue()
					+ ", getOrderdate()=" + getOrderdate() + ", getShippriority()=" + getShippriority() + "]";
		}
	}
}

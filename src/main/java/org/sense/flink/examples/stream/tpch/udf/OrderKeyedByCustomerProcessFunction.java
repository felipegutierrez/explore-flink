package org.sense.flink.examples.stream.tpch.udf;

import java.util.BitSet;
import java.util.List;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.examples.stream.tpch.pojo.Customer;
import org.sense.flink.examples.stream.tpch.pojo.Order;
import org.sense.flink.examples.stream.tpch.pojo.ShippingPriorityItem;
import org.sense.flink.util.CpuGauge;

import net.openhft.affinity.impl.LinuxJNAAffinity;

public class OrderKeyedByCustomerProcessFunction extends KeyedProcessFunction<Long, Order, ShippingPriorityItem> {
	private static final long serialVersionUID = 1L;

	private ListState<Customer> customerList = null;
	private transient CpuGauge cpuGauge;
	private BitSet affinity;
	private boolean pinningPolicy;

	public OrderKeyedByCustomerProcessFunction(boolean pinningPolicy) {
		this.pinningPolicy = pinningPolicy;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		this.cpuGauge = new CpuGauge();
		getRuntimeContext().getMetricGroup().gauge("cpu", cpuGauge);

		if (this.pinningPolicy) {
			// listing the cpu cores available
			int nbits = Runtime.getRuntime().availableProcessors();
			// pinning operator' thread to a specific cpu core
			this.affinity = new BitSet(nbits);
			affinity.set(((int) Thread.currentThread().getId() % nbits));
			LinuxJNAAffinity.INSTANCE.setAffinity(affinity);
		}

		ListStateDescriptor<Customer> customerDescriptor = new ListStateDescriptor<Customer>("customerState",
				Customer.class);
		customerList = getRuntimeContext().getListState(customerDescriptor);
	}

	@Override
	public void processElement(Order order, KeyedProcessFunction<Long, Order, ShippingPriorityItem>.Context context,
			Collector<ShippingPriorityItem> out) throws Exception {
		// updates the CPU core current in use
		this.cpuGauge.updateValue(LinuxJNAAffinity.INSTANCE.getCpu());

		if (customerList != null && Iterators.size(customerList.get().iterator()) == 0) {
			CustomerSource customerSource = new CustomerSource();
			List<Customer> customers = customerSource.getCustomers();
			customerList.addAll(customers);
		}

		for (Customer customer : customerList.get()) {
			// System.out.println("Customer: " + customer);
			// System.out.println("Order: " + order);
			if (order != null && order.getCustomerKey() == customer.getCustomerKey()) {
				try {
					ShippingPriorityItem spi = new ShippingPriorityItem(order.getOrderKey(), 0.0,
							OrdersSource.format(order.getOrderDate()), order.getShipPriority());
					// System.out.println("OrderProcessWindowFunction TRUE: " + spi);
					out.collect(spi);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}

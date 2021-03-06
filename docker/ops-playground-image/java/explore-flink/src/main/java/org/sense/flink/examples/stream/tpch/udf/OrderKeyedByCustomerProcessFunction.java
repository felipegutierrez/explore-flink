package org.sense.flink.examples.stream.tpch.udf;

import com.google.common.collect.ImmutableList;
import net.openhft.affinity.impl.LinuxJNAAffinity;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.examples.stream.tpch.pojo.Order;
import org.sense.flink.examples.stream.tpch.pojo.ShippingPriorityItem;
import org.sense.flink.util.CpuGauge;

import java.util.BitSet;

public class OrderKeyedByCustomerProcessFunction extends KeyedProcessFunction<Long, Order, ShippingPriorityItem> {
    private static final long serialVersionUID = 1L;
    private final boolean pinningPolicy;
    private ImmutableList<Long> customerKeyList = null;
    private transient CpuGauge cpuGauge;
    private BitSet affinity;

    public OrderKeyedByCustomerProcessFunction(boolean pinningPolicy) {
        this.pinningPolicy = pinningPolicy;
    }

    @Override
    public void open(Configuration parameters) {
        try {
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
            customerKeyList = ImmutableList.copyOf(new CustomerSource().getCustomersKeys());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void processElement(Order order, KeyedProcessFunction<Long, Order, ShippingPriorityItem>.Context context,
                               Collector<ShippingPriorityItem> out) {
        try {
            // updates the CPU core current in use
            this.cpuGauge.updateValue(LinuxJNAAffinity.INSTANCE.getCpu());

            for (Long customerKey : customerKeyList) {
                // System.out.println("Customer: " + customer + " - Order: " + order);
                if (order != null && order.getCustomerKey() == customerKey.longValue()) {
                    try {
                        ShippingPriorityItem spi = new ShippingPriorityItem(order.getOrderKey(), 0.0,
                                OrdersSource.format(order.getOrderDate()), order.getShipPriority());
                        out.collect(spi);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

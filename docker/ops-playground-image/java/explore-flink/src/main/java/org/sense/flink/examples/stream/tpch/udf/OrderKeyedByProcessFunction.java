package org.sense.flink.examples.stream.tpch.udf;

import com.google.common.collect.ImmutableList;
import net.openhft.affinity.impl.LinuxJNAAffinity;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.sense.flink.examples.stream.tpch.pojo.Order;
import org.sense.flink.util.CpuGauge;

import java.util.BitSet;

public class OrderKeyedByProcessFunction extends KeyedProcessFunction<Long, Order, Tuple2<Integer, Double>> {
    private static final long serialVersionUID = 1L;
    private final boolean pinningPolicy;
    private ImmutableList<Tuple2<Long, Double>> lineItemList = null;
    private transient CpuGauge cpuGauge;
    private BitSet affinity;

    public OrderKeyedByProcessFunction(boolean pinningPolicy) {
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
            lineItemList = ImmutableList.copyOf(new LineItemSource().getLineItemsRevenueByOrderKey());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void processElement(Order order, KeyedProcessFunction<Long, Order, Tuple2<Integer, Double>>.Context context,
                               Collector<Tuple2<Integer, Double>> out) {
        try {
            // updates the CPU core current in use
            this.cpuGauge.updateValue(LinuxJNAAffinity.INSTANCE.getCpu());

            for (Tuple2<Long, Double> lineItem : lineItemList) {
                if (lineItem.f0.equals(order.getOrderKey())) {
                    out.collect(Tuple2.of((int) order.getCustomerKey(), lineItem.f1));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

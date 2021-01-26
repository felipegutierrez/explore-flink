package org.sense.flink.examples.stream.tests;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class SocketWindowWordCountJava {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // @formatter:off
        env.socketTextStream("localhost", 9000)
                .flatMap(new SplitterFlatMap())
                .keyBy(new WordKeySelector())
                .reduce(new SumReducer())
                .keyBy(new WordKeySelector())
                .process(new SortKeyedProcessFunction(3 * 1000))
                .print().setParallelism(1);
        // @formatter:on

        String executionPlan = env.getExecutionPlan();
        System.out.println("ExecutionPlan ........................ ");
        System.out.println(executionPlan);
        System.out.println("........................ ");

        env.execute("Window WordCount sorted");
    }

    public static class SplitterFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 3121588720675797629L;

        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(Tuple2.of(word, 1));
            }
        }
    }

    public static class WordKeySelector implements KeySelector<Tuple2<String, Integer>, String> {

        @Override
        public String getKey(Tuple2<String, Integer> value) throws Exception {
            return value.f0;
        }
    }

    public static class SumReducer implements ReduceFunction<Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> event1, Tuple2<String, Integer> event2) throws Exception {
            return Tuple2.of(event1.f0, event1.f1 + event2.f1);
        }
    }

//    public static class EventKeySelector implements KeySelector<Event, String> {
//        @Override
//        public String getKey(Event event) throws Exception {
//            return event.getValue();
//        }
//    }

    public static class SortKeyedProcessFunction extends KeyedProcessFunction<String, Tuple2<String, Integer>, Event> {
        private static final long serialVersionUID = 7289761960983988878L;
        // delay after which an alert flag is thrown
        private final long timeOut;
        // state to remember the last timer set
        private ValueState<List<Event>> queueState = null;
        private ValueState<Long> lastTime = null;

        public SortKeyedProcessFunction(long timeOut) {
            this.timeOut = timeOut;
        }

        @Override
        public void open(Configuration conf) {
            // setup timer and HLL state
            ValueStateDescriptor<List<Event>> descriptor = new ValueStateDescriptor<>(
                    // state name
                    "sorted-events",
                    // type information of state
                    TypeInformation.of(new TypeHint<List<Event>>() {
                    }));
            queueState = getRuntimeContext().getState(descriptor);

            ValueStateDescriptor<Long> descriptorLastTime = new ValueStateDescriptor<Long>(
                    "lastTime",
                    TypeInformation.of(new TypeHint<Long>() {
                    }));

            lastTime = getRuntimeContext().getState(descriptorLastTime);
        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context context, Collector<Event> collector) throws Exception {
            // get current time and compute timeout time
            long currentTime = context.timerService().currentProcessingTime();
            long timeoutTime = currentTime + timeOut;
            // register timer for timeout time
            context.timerService().registerProcessingTimeTimer(timeoutTime);

            List<Event> queue = queueState.value();
            if (queue == null) {
                queue = new ArrayList<Event>();
            }
            Long current = lastTime.value();
            queue.add(new Event(value.f0, value.f1));
            lastTime.update(timeoutTime);
            queueState.update(queue);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Event> out) throws Exception {
            // System.out.println("onTimer: " + timestamp);
            // check if this was the last timer we registered
            System.out.println("timestamp: " + timestamp);
            List<Event> queue = queueState.value();
            Long current = lastTime.value();

            if (timestamp == current.longValue()) {
                Collections.sort(queue);

                queue.forEach( e -> {
                    out.collect(e);
                });
                queue.clear();

//                Event head = queue.poll();
//                while (head != null) {
//                    out.collect(head);
//                    queue.remove(head);
//                    // remove the next
//                    head = queue.poll();
//                }
                queueState.clear();
            }
        }
    }
}

//class EventComparator implements Comparator<Event> {
//    @Override
//    public int compare(Event t0, Event t1) {
//        return t1.getValue().compareTo(t0.getValue());
//    }
//}

class Event implements Comparable<Event> {
    String value;
    Integer qtd;

    public Event(String value, Integer qtd) {
        this.value = value;
        this.qtd = qtd;
    }

    public String getValue() {
        return value;
    }

    public Integer getQtd() {
        return qtd;
    }

    @Override
    public String toString() {
        return "Event{" +
                "value='" + value + '\'' +
                ", qtd=" + qtd +
                '}';
    }

    @Override
    public int compareTo(@NotNull Event event) {
        return this.getValue().compareTo(event.getValue());
    }
}

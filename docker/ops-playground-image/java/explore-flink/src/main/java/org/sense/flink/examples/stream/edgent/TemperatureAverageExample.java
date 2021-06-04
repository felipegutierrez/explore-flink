package org.sense.flink.examples.stream.edgent;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TemperatureAverageExample {

    private final TemperatureSourceFunction source;
    private final TemperatureSinkFunction sink;

    public TemperatureAverageExample(TemperatureSourceFunction source, TemperatureSinkFunction sink) throws Exception {
        this.source = source;
        this.sink = sink;
    }

    public static void main(String[] args) throws Exception {
        TemperatureAverageExample temperatureAverageExample = new TemperatureAverageExample(new TemperatureSourceFunction(),
                new TemperatureSinkFunction());
        temperatureAverageExample.execute();
    }

    public void execute() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        // env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // env.getConfig().disableSysoutLogging();

        // Tuple of (ID, READING)
        env.addSource(source)
                .keyBy(new TemperatureKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .aggregate(new AverageTemperatureAggregator())
                .map(new PrintMapFunction())
                .addSink(sink).setParallelism(1);

        String executionPlan = env.getExecutionPlan();
        System.out.println("ExecutionPlan ........................ ");
        System.out.println(executionPlan);
        System.out.println("........................ ");

        env.execute("TemperatureAverageExample");
    }

    public static class TemperatureSourceFunction implements SourceFunction<Tuple2<Integer, Double>> {

        private final List<Tuple2<Integer, Double>> temps;
        private final long milliseconds;

        public TemperatureSourceFunction() {
            this(500);
        }

        public TemperatureSourceFunction(List<Tuple2<Integer, Double>> temps) {
            this(temps, 500);
        }

        public TemperatureSourceFunction(long milliseconds) {
            this(Arrays.asList(new Tuple2<>(0, 10.0),
                    new Tuple2<>(1, 11.0), new Tuple2<>(0, 12.0), new Tuple2<>(2, 113.0), new Tuple2<>(3, 114.0),
                    new Tuple2<>(0, 16.0), new Tuple2<>(1, 19.0), new Tuple2<>(2, 18.0), new Tuple2<>(3, 15.0),
                    new Tuple2<>(1, 17.0), new Tuple2<>(1, 20.0), new Tuple2<>(2, 21.0), new Tuple2<>(3, 10.0),
                    new Tuple2<>(0, 18.0), new Tuple2<>(0, 19.0), new Tuple2<>(2, 18.0), new Tuple2<>(3, 13.0),
                    new Tuple2<>(1, 18.0), new Tuple2<>(0, 21.0), new Tuple2<>(2, 22.0), new Tuple2<>(3, 16.0),
                    new Tuple2<>(0, 16.0), new Tuple2<>(1, 21.0), new Tuple2<>(2, 22.0), new Tuple2<>(3, 12.0),
                    new Tuple2<>(0, 16.0), new Tuple2<>(0, 19.0), new Tuple2<>(2, 18.0), new Tuple2<>(3, 11.0)), milliseconds);
        }

        public TemperatureSourceFunction(List<Tuple2<Integer, Double>> temps, long milliseconds) {
            this.milliseconds = milliseconds;
            this.temps = temps;
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, Double>> ctx) throws Exception {
            for (Tuple2<Integer, Double> tuple2 : this.temps) {
                delay();
                ctx.collect(tuple2);
            }
        }

        private void delay() {
            try {
                Thread.sleep(milliseconds);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void cancel() {
        }
    }

    public static class TemperatureSinkFunction implements SinkFunction<Tuple2<Integer, Double>> {
        public static final Map<Integer, Double> result = new ConcurrentHashMap<Integer, Double>();

        public void invoke(Tuple2<Integer, Double> value, Context context) throws Exception {
            result.put(value.f0, value.f1);
        }
    }

    public class PrintMapFunction implements MapFunction<Tuple2<Integer, Double>, Tuple2<Integer, Double>> {
        @Override
        public Tuple2<Integer, Double> map(Tuple2<Integer, Double> value) throws Exception {
            System.out.println("ID: " + value.f0 + " - temperature: " + value.f1);
            return value;
        }
    }

    public class TemperatureKeySelector implements KeySelector<Tuple2<Integer, Double>, Integer> {
        // Tuple of (ID, READING)
        @Override
        public Integer getKey(Tuple2<Integer, Double> value) throws Exception {
            return value.f0;
        }
    }

    public class AverageTemperatureAggregator implements AggregateFunction<Tuple2<Integer, Double>,
            Tuple3<Integer, Double, Integer>,
            Tuple2<Integer, Double>> {
        // Tuple of (ID, READING)
        // Tuple of (ID, READING, COUNT)
        @Override
        public Tuple3<Integer, Double, Integer> createAccumulator() {
            return Tuple3.of(0, 0.0, 0);
        }

        @Override
        public Tuple3<Integer, Double, Integer> add(Tuple2<Integer, Double> value,
                                                    Tuple3<Integer, Double, Integer> accumulator) {
            return Tuple3.of(value.f0, accumulator.f1 + value.f1, accumulator.f2 + 1);
        }

        @Override
        public Tuple2<Integer, Double> getResult(Tuple3<Integer, Double, Integer> accumulator) {
            return Tuple2.of(accumulator.f0, (accumulator.f1 / (double) accumulator.f2));
        }

        @Override
        public Tuple3<Integer, Double, Integer> merge(Tuple3<Integer, Double, Integer> a,
                                                      Tuple3<Integer, Double, Integer> b) {
            return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
        }
    }
}

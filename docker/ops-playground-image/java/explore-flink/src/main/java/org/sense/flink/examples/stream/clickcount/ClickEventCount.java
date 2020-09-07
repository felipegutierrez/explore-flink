/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sense.flink.examples.stream.clickcount;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.sense.flink.examples.stream.clickcount.functions.BackpressureMap;
import org.sense.flink.examples.stream.clickcount.functions.ClickEventStatisticsCollector;
import org.sense.flink.examples.stream.clickcount.functions.CountingAggregator;
import org.sense.flink.examples.stream.clickcount.records.ClickEvent;
import org.sense.flink.examples.stream.clickcount.records.ClickEventDeserializationSchema;
import org.sense.flink.examples.stream.clickcount.records.ClickEventStatistics;
import org.sense.flink.examples.stream.clickcount.records.ClickEventStatisticsSerializationSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * A simple streaming job reading {@link ClickEvent}s from Kafka, counting events per 15 seconds and
 * writing the resulting {@link ClickEventStatistics} back to Kafka.
 *
 * <p> It can be run with or without checkpointing and with event time or processing time semantics.
 * </p>
 *
 * <p>The Job can be configured via the command line:</p>
 * * "--checkpointing": enables checkpointing
 * * "--event-time": set the StreamTimeCharacteristic to EventTime
 * * "--backpressure": insert an operator that causes periodic backpressure
 * * "--input-topic": the name of the Kafka Topic to consume {@link ClickEvent}s from
 * * "--output-topic": the name of the Kafka Topic to produce {@link ClickEventStatistics} to
 * * "--bootstrap.servers": comma-separated list of Kafka brokers
 */
public class ClickEventCount {

    public static final String CHECKPOINTING_OPTION = "checkpointing";
    public static final String EVENT_TIME_OPTION = "event-time";
    public static final String BACKPRESSURE_OPTION = "backpressure";

    public static final Time WINDOW_SIZE = Time.of(15, TimeUnit.SECONDS);
    private final String inputTopic;
    private final String outputTopic;
    private final String brokers;
    private final boolean inflictBackpressure;
    private final boolean checkpointingEnabled;
    private final boolean eventTimeSemantics;

    public ClickEventCount() throws Exception {
        this("input", "output", "kafka", 9092, true, true, true);
    }

    public ClickEventCount(String inputTopic, String outputTopic, String kafkaBroker, int kafkaPort, boolean inflictBackpressure, boolean checkpointingEnabled
            , boolean eventTimeSemantics) throws Exception {
        Map<String, String> data = new HashMap<String, String>();
        data.put("input-topic", inputTopic);
        data.put("output-topic", outputTopic);
        data.put("bootstrap.servers", kafkaBroker + ":" + kafkaPort);

        final ParameterTool params = ParameterTool.fromMap(data);
        this.inflictBackpressure = inflictBackpressure;
        this.checkpointingEnabled = checkpointingEnabled;
        this.eventTimeSemantics = eventTimeSemantics;
        this.inputTopic = params.get("input-topic", "input");
        this.outputTopic = params.get("output-topic", "output");
        this.brokers = params.get("bootstrap.servers", "localhost:9092");
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "click-event-count");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (checkpointingEnabled) {
            env.enableCheckpointing(1000);
        }
        if (eventTimeSemantics) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        }
        //disabling Operator chaining to make it easier to follow the Job in the WebUI
        env.disableOperatorChaining();

        DataStream<ClickEvent> clicks =
                env.addSource(new FlinkKafkaConsumer<>(inputTopic, new ClickEventDeserializationSchema(), kafkaProps))
                        .name("ClickEvent Source")
                        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ClickEvent>(Time.of(200, TimeUnit.MILLISECONDS)) {
                            @Override
                            public long extractTimestamp(final ClickEvent element) {
                                return element.getTimestamp().getTime();
                            }
                        });

        if (inflictBackpressure) {
            // Force a network shuffle so that the backpressure will affect the buffer pools
            clicks = clicks
                    .keyBy(ClickEvent::getPage)
                    .map(new BackpressureMap())
                    .name(BackpressureMap.class.getSimpleName());
        }

        DataStream<ClickEventStatistics> statistics = clicks
                .keyBy(ClickEvent::getPage)
                .timeWindow(WINDOW_SIZE)
                .aggregate(new CountingAggregator(),
                        new ClickEventStatisticsCollector())
                .name(CountingAggregator.class.getSimpleName());

        statistics
                .addSink(new FlinkKafkaProducer<>(
                        outputTopic,
                        new ClickEventStatisticsSerializationSchema(outputTopic),
                        kafkaProps,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
                .name(FlinkKafkaProducer.class.getSimpleName());

        env.execute(ClickEventCount.class.getSimpleName());
    }

    public static void main(String[] args) throws Exception {
        new ClickEventCount();
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sense.flink.examples.stream.clickcount;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.sense.flink.examples.stream.clickcount.records.ClickEvent;
import org.sense.flink.examples.stream.clickcount.records.ClickEventSerializationSchema;

import java.util.*;

import static org.sense.flink.examples.stream.clickcount.ClickEventCount.WINDOW_SIZE;


/**
 * A generator which pushes {@link ClickEvent}s into a Kafka Topic configured via `--topic` and
 * `--bootstrap.servers`.
 *
 * <p> The generator creates the same number of {@link ClickEvent}s for all pages. The delay between
 * events is chosen such that processing time and event time roughly align. The generator always
 * creates the same sequence of events. </p>
 */
public class ClickEventGenerator {

    public static final int EVENTS_PER_WINDOW = 1000;

    private static final List<String> pages = Arrays.asList("/help", "/index", "/shop", "/jobs", "/about", "/news");

    //this calculation is only accurate as long as pages.size() * EVENTS_PER_WINDOW divides the
    //window size
    public static final long DELAY = WINDOW_SIZE.toMilliseconds() / pages.size() / EVENTS_PER_WINDOW;

    private final String topic;

    public ClickEventGenerator() throws InterruptedException {
        this("input", "kafka", 9092);
    }

    public ClickEventGenerator(String topic, String kafkaBroker, int kafkaPort) throws InterruptedException {
        this.topic = topic;

        Map<String, String> data = new HashMap<String, String>();
        data.put("bootstrap.servers", kafkaBroker + ":" + kafkaPort);
        Properties kafkaProps = createKafkaProperties(ParameterTool.fromMap(data));

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(kafkaProps);

        ClickIterator clickIterator = new ClickIterator();

        while (true) {
            ProducerRecord<byte[], byte[]> record = new ClickEventSerializationSchema(topic).serialize(clickIterator.next(), null);
            producer.send(record);
            Thread.sleep(DELAY);
        }
    }

    public static void main(String[] args) throws Exception {
        new ClickEventGenerator();
    }

    private static Properties createKafkaProperties(final ParameterTool params) {
        String brokers = params.get("bootstrap.servers", "localhost:9092");
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        return kafkaProps;
    }

    static class ClickIterator {

        private final Map<String, Long> nextTimestampPerKey;
        private int nextPageIndex;

        ClickIterator() {
            nextTimestampPerKey = new HashMap<>();
            nextPageIndex = 0;
        }

        ClickEvent next() {
            String page = nextPage();
            return new ClickEvent(nextTimestamp(page), page);
        }

        private Date nextTimestamp(String page) {
            long nextTimestamp = nextTimestampPerKey.getOrDefault(page, 0L);
            nextTimestampPerKey.put(page, nextTimestamp + WINDOW_SIZE.toMilliseconds() / EVENTS_PER_WINDOW);
            return new Date(nextTimestamp);
        }

        private String nextPage() {
            String nextPage = pages.get(nextPageIndex);
            if (nextPageIndex == pages.size() - 1) {
                nextPageIndex = 0;
            } else {
                nextPageIndex++;
            }
            return nextPage;
        }
    }
}

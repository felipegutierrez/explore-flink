package org.sense.flink.examples.stream.tpch;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.sense.flink.examples.stream.tpch.pojo.Order;
import org.sense.flink.util.DataRateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.sense.flink.util.MetricLabels.TPCH_DATA_ORDER;

public class TCPHOrderProducer {

    private static final String TOPIC_ORDERS = "tpc-h-order";
    private final String topic;
    private final Logger logger = LoggerFactory.getLogger(TCPHOrderProducer.class);
    private final String dataFilePath;
    private final DataRateListener dataRateListener;
    private final String bootstrapServers;
    private Boolean running;

    public TCPHOrderProducer() {
        this(TPCH_DATA_ORDER, "127.0.0.1:9092", TOPIC_ORDERS);
    }

    public TCPHOrderProducer(String bootstrapServers) {
        this(TPCH_DATA_ORDER, bootstrapServers, TOPIC_ORDERS);
    }

    public TCPHOrderProducer(String bootstrapServers, String topic) {
        this(TPCH_DATA_ORDER, bootstrapServers, topic);
    }

    public TCPHOrderProducer(String dataFilePath, String bootstrapServers, String topic) {
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
        this.dataRateListener = new DataRateListener();
        this.dataRateListener.start();

        this.running = true;

        if (dataFilePath == null) {
            this.dataFilePath = TPCH_DATA_ORDER;
        } else {
            this.dataFilePath = dataFilePath;
        }

        disclaimer();
        // create properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // producer acks
        // properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        // properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        // properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        // leverage idempotent
        // properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        int i = 0;
        while (running) {
            logger.info("Producing batch: " + i);
            try {
                System.out.println("reading file: " + dataFilePath);
                InputStream stream = new FileInputStream(dataFilePath);
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));

                int rowNumber = 0;
                long startTime = System.nanoTime();
                String line = reader.readLine();
                while (line != null) {
                    rowNumber++;
                    Order order = getOrderItem(line, rowNumber);
                    // create producer record
                    final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, String.valueOf(order.getOrderKey()), order.getJson().toString());

                    // send data asynchronous
                    producer.send(record, new Callback() {
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            // executes every time that a record is sent successfully
                            if (e == null) {
                                // the record was successfully sent
                                logger.info("Received metadata: Topic: " + recordMetadata.topic() +
                                        " Partition: " + recordMetadata.partition() +
                                        " Offset: " + recordMetadata.offset() +
                                        " Timestamp: " + recordMetadata.timestamp());
                            } else {
                                logger.error("Error on sending message: " + e.getMessage());
                            }
                        }
                    });
                    i++;

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
                this.running = false;
            } catch (IOException e) {
                e.printStackTrace();
                this.running = false;
            }
        }
        producer.close();
    }

    public static void main(String[] args) {
        String exampleTopic = "my-topic";
        String bootserverStrapExample = "127.0.0.1:9092"; //"10.98.121.13:9092";
        TCPHOrderProducer tcpHOrderProducer = new TCPHOrderProducer(bootserverStrapExample, exampleTopic);
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

    private void disclaimer() {
        System.out.println("dataFilePath:     " + this.dataFilePath);
        System.out.println("bootstrapServers: " + this.bootstrapServers);
        System.out.println("topic:            " + this.topic);
        System.out.println();
    }
}

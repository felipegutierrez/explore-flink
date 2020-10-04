
# explore-flink project

```
mvn clean package
```

Start the zookeeper, Kafka brokers, create topics:
```
./bin/zookeeper-server-start.sh config/zookeeper.properties
./bin/kafka-server-start.sh config/server.properties
./bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic tpc-h-order --partitions 6 --replication-factor 1
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic tpc-h-order

```

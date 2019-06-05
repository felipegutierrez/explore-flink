
This project is based on [Apache Flink](https://flink.apache.org/) and it is consuming data from another project [https://github.com/felipegutierrez/explore-rpi](https://github.com/felipegutierrez/explore-rpi) which is based on [Apache Edgent](http://edgent.apache.org/).


## Instructions to execute

### Compile the project

```
cd explore-flink/
mvn clean package
```

### Flink cluster

Setup a [Flink standalone cluster](https://ci.apache.org/projects/flink/flink-docs-release-1.8/tutorials/local_setup.html). This project was tested with Flink version 1.8.0.

Download the required libraries and copy them to the Flink cluster `lib` directory. Make sure that you download the librarie version corresponding to the same version of Flink libraries.

 - [Flink Metrics Dropwizard](https://mvnrepository.com/artifact/org.apache.flink/flink-metrics-dropwizard)
 - [Flink Metrics Prometheus](https://mvnrepository.com/artifact/org.apache.flink/flink-metrics-prometheus)
 - [Metrics Core](https://mvnrepository.com/artifact/io.dropwizard.metrics/metrics-core)
 - [MQTT Client](https://mvnrepository.com/artifact/org.fusesource.mqtt-client/mqtt-client)

```
$ ll flink-1.7.2/lib/
total 92268
drwxrwxr-x  2 flink flink     4096 Apr  8 15:55 ./
drwxrwxr-x 10 flink flink     4096 Apr  8 13:07 ../
-rw-r--r--  1 flink flink 93445474 Feb 11 16:38 flink-dist_2.11-1.8.0.jar
-rw-rw-r--  1 flink flink    17739 Apr  8 15:55 flink-metrics-dropwizard-1.8.0.jar
-rw-rw-r--  1 flink flink   102760 Mär 29 16:31 flink-metrics-prometheus_2.11-1.8.0.jar
-rw-r--r--  1 flink flink   141937 Feb 11 16:37 flink-python_2.11-1.8.0.jar
-rw-rw-r--  1 flink flink   489884 Feb 11 15:32 log4j-1.2.17.jar
-rw-rw-r--  1 flink flink   120465 Apr  8 15:54 metrics-core-3.1.5.jar
-rw-rw-r--  1 flink flink   126953 Mär 29 15:06 mqtt-client-1.15.jar
-rw-rw-r--  1 flink flink     9931 Feb 11 15:32 slf4j-log4j12-1.7.15.jar
```

I am using the configuration below on the `conf/flink-conf.yaml`. This file is also used to configure Prometheus with FLink.
```
jobmanager.rpc.address: IP_OF_THE_MASTER_NODE
rest.address: IP_OF_THE_MASTER_NODE
taskmanager.numberOfTaskSlots: 4
parallelism.default: 4
taskmanager.tmp.dirs: /home/flink/Server/tmp/
metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
metrics.reporter.prom.host: IP_OF_THE_MASTER_NODE
metrics.reporter.prom.port: 9250-9260
```

### Starting the data source project

We need to generate data in order to our Flink application consume and analyse it. Due to it, we are going to use [another project](https://github.com/felipegutierrez/explore-rpi) which is based on Apache Edgent. The command below shows how to start an application which is a data source for our Flink application. It sends data to a MQTT broker and Flink consumes it. We are using the application number `11` which generates data in specific MQTT topics which are consument by the Flink application number `18`.

`java -jar target/explore-rpi.jar 11`

### Deploy the application on the Flink cluster

Here we are deploying the application number 18 on the Flink cluster:

`./bin/flink run -c org.sense.flink.App explore-flink.jar 18 IP_OF_MQTT_DATA_SOURCE IP_OF_MQTT_SINK &`

Then we can listen to the output of the sing in a MQTT sink using the command below. The `IP_OF_MQTT_SINK` below has to be the some of the above. The topic `topic-data-skewed-join` id defined on the application 18 [MqttSensorDataSkewedCombinerByKeySkewedDAG](https://github.com/felipegutierrez/explore-flink/blob/master/src/main/java/org/sense/flink/examples/stream/MqttSensorDataSkewedCombinerByKeySkewedDAG.java#L30).

`mosquitto_sub -h IP_OF_MQTT_SINK -t topic-data-skewed-join`


### Remove the application from the Flink cluster

List all the applications that are running on the Flink cluster and chose the one that you wish to cancel.

```
./bin/flink list
./bin/flink cancel ID_OF_THE_APPLICATION_RUNNING_ON_THE_CLUSTER
```

### Testing the Cardinality estimation using strem-lib library

[stream-lib](https://github.com/addthis/stream-lib) is a Java library for summarizing data in streams for which it is infeasible to store all events. The class below test this library with out running it on Flink.

```
java -cp target/explore-flink.jar org.sense.flink.examples.stream.TestHyperLogLog
```

### Troubleshooting






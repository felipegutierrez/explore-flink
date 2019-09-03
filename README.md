
This project is based on [Apache Flink](https://flink.apache.org/) and it is consuming data from another project [https://github.com/felipegutierrez/explore-rpi](https://github.com/felipegutierrez/explore-rpi) which is based on [Apache Edgent](http://edgent.apache.org/).

## Requirements

 - Java 8
 - Scala 2.11
 - Mqtt broker
 - Flink 1.9.0 standalone cluster

### Mqtt broker

```
sudo apt install mosquitto mosquitto-clients
```

### Flink cluster

Setup a [Flink standalone cluster](https://ci.apache.org/projects/flink/flink-docs-release-1.8/tutorials/local_setup.html). This project was tested with Flink version 1.9.0.

Download the required libraries and copy them to the Flink cluster `lib` directory. Make sure that you download the librarie version corresponding to the same version of Flink libraries. It is necessary to download additional libraries for [Prometheus](https://ci.apache.org/projects/flink/flink-docs-release-1.9/monitoring/metrics.html#prometheus-orgapacheflinkmetricsprometheusprometheusreporter) and for [system resource metrics](https://ci.apache.org/projects/flink/flink-docs-release-1.9/monitoring/metrics.html#system-resources). All libraries are listed below.

 - [Flink Metrics Dropwizard](https://mvnrepository.com/artifact/org.apache.flink/flink-metrics-dropwizard)
 - [Flink Metrics Prometheus](https://mvnrepository.com/artifact/org.apache.flink/flink-metrics-prometheus)
 - [Metrics Core](https://mvnrepository.com/artifact/io.dropwizard.metrics/metrics-core)
 - [MQTT Client](https://mvnrepository.com/artifact/org.fusesource.mqtt-client/mqtt-client)
 - [OSHI Core](https://mvnrepository.com/artifact/com.github.oshi/oshi-core/3.4.0)
 - [Java Native Access Platform](https://mvnrepository.com/artifact/net.java.dev.jna/jna-platform/4.2.2)
 - [Java Native Access](https://mvnrepository.com/artifact/net.java.dev.jna/jna/4.2.2)

```
$ ll flink-1.9.0/lib/
total 138448
drwxrwxr-x  2 flink flink     4096 Sep  3 16:23 ./
drwxrwxr-x 11 flink flink     4096 Aug 28 09:41 ../
-rw-r--r--  1 flink flink 96634700 Aug 19 18:55 flink-dist_2.11-1.9.0.jar
-rw-r--r--  1 flink flink    17732 Aug 23 16:50 flink-metrics-dropwizard-1.9.0.jar
-rw-r--r--  1 flink flink   103759 Aug 23 16:50 flink-metrics-prometheus_2.11-1.9.0.jar
-rw-r--r--  1 flink flink 18739722 Aug 19 18:54 flink-table_2.11-1.9.0.jar
-rw-r--r--  1 flink flink 22175615 Aug 19 18:55 flink-table-blink_2.11-1.9.0.jar
-rw-rw-r--  1 flink flink  1137286 Sep  3 16:23 jna-4.2.2.jar
-rw-rw-r--  1 flink flink  1856200 Sep  3 16:23 jna-platform-4.2.2.jar
-rw-rw-r--  1 flink flink   489884 Aug 19 18:22 log4j-1.2.17.jar
-rw-rw-r--  1 flink flink   104225 Aug 22 16:34 metrics-core-4.1.0.jar
-rw-r--r--  1 flink flink   126953 Aug 22 16:34 mqtt-client-1.15.jar
-rw-rw-r--  1 flink flink   338636 Sep  3 16:23 oshi-core-3.4.0.jar
-rw-rw-r--  1 flink flink     9931 Aug 19 18:22 slf4j-log4j12-1.7.15.jar
```
### Exporting data to Prometheus

I am using the configuration below on the `flink-1.9.0/conf/flink-conf.yaml`. This file is also used to configure Prometheus with FLink.
```
jobmanager.rpc.address: IP_OF_THE_MASTER_NODE
rest.address: IP_OF_THE_MASTER_NODE
taskmanager.numberOfTaskSlots: 4
parallelism.default: 4
taskmanager.tmp.dirs: /home/flink/Server/tmp/

## Prometheus configuration
metrics.reporter.prom.class: org.apache.flink.metrics.prometheus.PrometheusReporter
metrics.reporter.prom.host: IP_OF_THE_MASTER_NODE
metrics.reporter.prom.port: 9250-9260
```


## Instructions to execute

### Compile the project

```
cd explore-flink/
mvn clean package
```
or skiping the tests
```
mvn clean package -DskipTests
```

### Starting the data source project

This subsection is useful only if you are aiming to use the data source project with apache Edgend. If you want to use an application which collects data form the internet you can skipt this subsection.
We need to generate data in order to our Flink application consume and analyse it. Due to it, we are going to use [another project](https://github.com/felipegutierrez/explore-rpi) which is based on Apache Edgent. The command below shows how to start an application which is a data source for our Flink application. It sends data to a MQTT broker and Flink consumes it. We are using the application number `11` which generates data in specific MQTT topics which are consument by the Flink application number `18`.

`java -jar target/explore-rpi.jar 11`

### Deploy the application on the Flink cluster

Here we are deploying the application number 30 on the Flink cluster. We are also sending parameters with the source and sink IP address, the frequency of pulling data from the sources, a flag to inject synthetic data on the fly, and the frequency of the processing window.

```
./bin/flink run -c org.sense.flink.App ../app/explore-flink.jar -app 30 -source 127.0.0.1 -sink 127.0.0.1 \
-offlineData true -frequencyPull 10 -frequencyWindow 30 -syntheticData true
```
Or if you define the variables on a bash script:
```
FLINK_HOME=/home/flink/flink-1.9.0
FLINK_CLI=/home/flink/flink-1.9.0/bin/flink
FLINK_APP=/home/flink/app/explore-flink.jar

echo `$FLINK_CLI run -c org.sense.flink.App $FLINK_APP -app 30 -source 127.0.0.1 -sink 127.0.0.1 \
-offlineData true -frequencyPull 10 -frequencyWindow 30 -syntheticData true`
```

Then, the application `30` for example, has a channel to receive a frequency parameter which changes dynamically.
```
mosquitto_pub -h 127.0.0.1 -t topic-synthetic-frequency-pull -m "TRAFFIC_JAM 1000"
mosquitto_pub -h 127.0.0.1 -t topic-synthetic-frequency-pull -m "AIR_POLLUTION 500"
```

You can subscribe to the channel of each application in order to consume its data. For example:
`mosquitto_sub -h 127.0.0.1 -t topic-valencia-data-cpu-intensive`


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






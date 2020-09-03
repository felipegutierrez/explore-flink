
[![Build Status](https://api.travis-ci.org/felipegutierrez/explore-flink.svg?branch=master)](https://travis-ci.org/felipegutierrez/explore-flink)

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

### Flink on the cluster

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

### Flink + Mesos on the cluster

You have to configure the `conf/flink-conf.yaml` file according to the [official documentation](https://ci.apache.org/projects/flink/flink-docs-stable/ops/deployment/mesos.html#mesos-without-dcos). Mesos has the ability to deliver resources dynamically to Flink as it asks. The property `mesos.resourcemanager.tasks.cpus` cannot exceed the number of cores of a node in the cluster, otherwise the TaskManager will not start. Additionaly, you have to download a [Pre-bundled Hadoop X.X.X](https://flink.apache.org/downloads.html#apache-flink-190) library and copy it to the `lib` directory.

```
#===============================================================================
#Mesos configuration
#=============================================================================
mesos.master: 127.0.0.1:5050
mesos.initial-tasks: 2
mesos.resourcemanager.tasks.container.type: mesos
jobmanager.heap.mb: 1024
jobmanager.web.address: 127.0.0.1
jobmanager.web.port: 8081
#   mesos.resourcemanager.tasks.mem: 4096
#   taskmanager.heap.mb: 3500
mesos.resourcemanager.tasks.cpus: 5.0
#   mesos.resourcemanager.tasks.disk: 4096
#   mesos.resourcemanager.framework.name: "FLINK_on_MESOS_intensive_cpu_usage"
```

Load the mesos libs on your environment and start Flink cluster on Mesos resource manager.
```
export MESOS_NATIVE_JAVA_LIBRARY="/home/felipe/workspace-vsc/mesos/build/src/.libs/libmesos.so"
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/felipe/workspace-vsc/mesos/build/src/.libs/
PATH=$PATH:/home/felipe/workspace-vsc/mesos/build/bin/
```

Then start Flink on Mesos cluster and deploy an application.
```
/home/flink/flink-1.9.0/bin/mesos-appmaster.sh &
./bin/flink run /home/flink/hello-flink-mesos/target/hello-flink-mesos.jar &
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
### Executing a CPU intensive stream application

The producers are detached from the stream application. The purpose of this is to isolate the producer from the consumers. Moreover, we can change the pooling frequency of the producers in run-time. This facilitates to analyse the behaviour of the stream application when there is a workload variation without restart it. The Producers are the applications `32` (traffic data from Valencia Open-data web portal) and `33` (air pollution data from Valencia Open-data web portal). The producers publish data on the MQTT broker and the stream application consumes data from the MQTT broker.

#### Consumers

We are going to begin with the consumers because the producers will run with a limited dataset. So it is better to launch the consumer stream application first and keep it listenning to data from the MQTT broker. 

The CPU intensive stream application is the number `34` and we use the class `org.sense.flink.App` to call this application. We can set other parameters to start the stream application. For instance, window size, ip of the source and sink, degree of parallelism (it means the numbe of physical instances of each operator running on the cluster), type of the output which can be a file or a MQTT channel. Below is the pattern to call the application and a detail description of each parameter.
```
/home/flink/flink-1.9.0/bin/flink run -c org.sense.flink.App \
	/home/flink/explore-flink/target/explore-flink.jar -app 34 \
	-source 130.239.48.136 -sink 130.239.48.136 -frequencyWindow [seconds] \
	-parallelism [int] -disableOperatorChaining [true|false] \
	-pinningPolicy [true|false] -output [file|mqtt] &
```
Description of each parameter:
 - `-app`: which application to deploy. If you don't pass any parameter the jar file will output all applications available.
 - `-source` or `-sink`: IP of the source and sink nodes. It means that you can see the output of the application on the machines that hold these IPs.
 - `-frequencyWindow`: frequency to compute the window in seconds.
 - `-parallelism`: degree of parallelism to deploy the application on the cluster. It means the redundante operators will be created in order to guarantee fault tolerance.
 - `-disableOperatorChaining`: FALSE is the default. TRUE disables fusion optimization for all operators which means that operators will be allocated on different threads (https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/config.html#configuring-taskmanager-processing-slots).
 - `-pinningPolicy`: TRUE enables the strategy to pinning operator' threads to specific CPU cores. FALSE is the deault.
 - `-output`: 'file' means that the output will be generated in the Flink TaskManagers log files. 'mqtt' means that you have to subscribe to a mqtt channel according to the message showed when the application is deployed.

Here is a workable example of calling the CPU intensive stream application on the Flink standalone cluster.
```
/home/flink/flink-1.9.0/bin/flink run -c org.sense.flink.App \
	/home/flink/explore-flink/target/explore-flink.jar -app 34 \
	-source 130.239.48.136 -sink 130.239.48.136 -frequencyWindow 60 \
	-parallelism 4 -disableOperatorChaining true -pinningPolicy true \
	-output mqtt &
```
The stream application will terminate when it receives the `SHUTDOWN` flag from all the sources that it is consuming data. This example uses 2 data sources (traffic and air pollution data). So, you have to launch both producers with the `-maxCount` parameter otherwise the stream application will not receive the `SHUTDOWN` signal to finish. Note that if you forget to start one of the producers with the `-maxCount` parameter, the stream application will run indefinitely.

#### Producers

The producer application receives parameters as arguments: `org.sense.flink.App -app [id of the application] -offlineData [true|false] -maxCount [times to read the data source]`. For the examples below we are sending 3 times the data from a offline data file. Once the producer counts 3 times it sends a `SHUTDOWN` signal to the MQTT broker.
```
java -classpath /home/flink/flink-1.9.0/lib/flink-dist_2.11-1.9.0.jar:/home/flink/explore-flink/target/explore-flink.jar \
	org.sense.flink.App -app 32 -offlineData true -maxCount 3 &
java -classpath /home/flink/flink-1.9.0/lib/flink-dist_2.11-1.9.0.jar:/home/flink/explore-flink/target/explore-flink.jar \
	org.sense.flink.App -app 33 -offlineData true -maxCount 3 &
```
Once you started the producer applications you can change the frequency of producing data in run-time. The frequency is changed by updating the delay between sending data to the MQTT broker. When we start the producer the default delay is 10 thousand milliseconds. It means that every 10 seconds we are sending new data to the broker. The two commands below change the delay to 1 and 5 thousand milliseconds. It means that we are sending every second and five seconds data to the broker.
```
mosquitto_pub -h localhost -p 1883 -t topic-valencia-traffic-jam-frequency -m '1000'
mosquitto_pub -h localhost -p 1883 -t topic-valencia-pollution-frequency -m '5000'
```
Note that changing the frequency of sending data to the broker will make the producer finish early if a `-maxCount` parameter was set when the producer has been launched.


### Starting the data source project with Apache Edgent

This subsection is useful only if you are aiming to use the data source project with apache Edgend. If you want to use an application which collects data form the internet you can skipt this subsection.
We need to generate data in order to our Flink application consume and analyse it. Due to it, we are going to use [another project](https://github.com/felipegutierrez/explore-rpi) which is based on Apache Edgent. The command below shows how to start an application which is a data source for our Flink application. It sends data to a MQTT broker and Flink consumes it. We are using the application number `11` which generates data in specific MQTT topics which are consument by the Flink application number `18`.

`java -jar target/explore-rpi.jar 11`

### Deploy the application on the Flink cluster

You can execute the following script to receive instructions on how to deploy an application with several arguments on the cluster by executing the command: `bash ./conf/launchApp.sh` on your terminal. Otherwise you can follow this section.
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






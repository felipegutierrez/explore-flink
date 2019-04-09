
This project is based on [Apache Flink](https://flink.apache.org/) and it is consuming data from another project [https://github.com/felipegutierrez/explore-rpi](https://github.com/felipegutierrez/explore-rpi) which is based on [Apache Edgent](http://edgent.apache.org/).


## Instructions to execute

### Compile the project

```
cd explore-flink/
mvn clean package
```

### Flink cluster

Copy the required libraries to the Flink cluster `lib` directory.

```
$ ll flink-1.7.2/lib/
total 92268
drwxrwxr-x  2 flink flink     4096 Apr  8 15:55 ./
drwxrwxr-x 10 flink flink     4096 Apr  8 13:07 ../
-rw-r--r--  1 flink flink 93445474 Feb 11 16:38 flink-dist_2.11-1.7.2.jar
-rw-rw-r--  1 flink flink    17739 Apr  8 15:55 flink-metrics-dropwizard-1.7.2.jar
-rw-rw-r--  1 flink flink   102760 Mär 29 16:31 flink-metrics-prometheus_2.11-1.7.2.jar
-rw-r--r--  1 flink flink   141937 Feb 11 16:37 flink-python_2.11-1.7.2.jar
-rw-rw-r--  1 flink flink   489884 Feb 11 15:32 log4j-1.2.17.jar
-rw-rw-r--  1 flink flink   120465 Apr  8 15:54 metrics-core-3.1.5.jar
-rw-rw-r--  1 flink flink   126953 Mär 29 15:06 mqtt-client-1.15.jar
-rw-rw-r--  1 flink flink     9931 Feb 11 15:32 slf4j-log4j12-1.7.15.jar
```

### Deploy the application on the Flink cluster

`./bin/flink run -c explore-flink-.jar org.sense.flink.App 14 IP_OF_MQTT_DATA_SOURCE IP_OF_MQTT_SINK`

Open the Flink dashboard to see your application running or execute `./bin/flink list`

### Remove the application from the Flink cluster

```
./bin/flink list
./bin/flink cancel ID_OF_THE_APPLICATION_RUNNING_ON_THE_CLUSTER
```

### Troubleshooting






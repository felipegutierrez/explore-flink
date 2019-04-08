
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
flink-metrics-dropwizard_2.11-1.7.2.jar
mqtt-client-1.15.jar
flink-metrics-prometheus_2.11-1.7.2.jar
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






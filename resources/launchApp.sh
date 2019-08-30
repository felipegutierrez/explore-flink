#!/bin/bash
#set -x
#set -v

FLINK_HOME=/home/flink/flink-1.9.0
FLINK_CLI=/home/flink/flink-1.9.0/bin/flink
FLINK_APP=/home/flink/explore-flink/target/explore-flink.jar

echo
echo "Launching a Flink Stream application >>"
echo "$FLINK_CLI run -c org.sense.flink.App $FLINK_APP -app 30 -source 130.239.48.136 -sink 130.239.48.136 -offlineData true -frequencyPull 10 -frequencyWindow 30 -syntheticData true"
echo

echo
echo "Consuming a Flink Stream application output >>"
echo "mosquitto_sub -h 130.239.48.136 -t topic-valencia-data-cpu-intensive"
echo

echo "Listing all Flink Stream applications"
echo "$FLINK_CLI list"
echo

echo "Canceling a Flink Stream application"
echo "$FLINK_CLI cancel APP_ID"
echo

echo "Sending parameters to change the frequency of synthetic item generators"
echo "mosquitto_pub -h 127.0.0.1 -t topic-synthetic-frequency-pull -m \"AIR_POLLUTION 500\""
echo "mosquitto_pub -h 127.0.0.1 -t topic-synthetic-frequency-pull -m \"TRAFFIC_JAM 1000\""
echo "mosquitto_pub -h 127.0.0.1 -t topic-synthetic-frequency-pull -m \"NOISE 600\""
echo



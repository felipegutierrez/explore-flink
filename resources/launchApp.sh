#!/bin/bash
#set -x
#set -v

FLINK_HOME=/home/flink/flink-1.9.0
FLINK_CLI=/home/flink/flink-1.9.0/bin/flink
FLINK_APP=/home/flink/explore-flink/target/explore-flink.jar

echo
echo "Launching a Flink Stream application >>"
echo "$FLINK_CLI run -c org.sense.flink.App $FLINK_APP -app 30 -source 127.0.0.1 -sink 127.0.0.1 -offlineData true -frequencyPull 10 -frequencyWindow 30 -syntheticData true"
echo

echo "Listing all Flink Stream applications"
echo "$FLINK_CLI list"
echo

echo "Canceling a Flink Stream application"
echo "$FLINK_CLI cancel APP_ID"
echo

